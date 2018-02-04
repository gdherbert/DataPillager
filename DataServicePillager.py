# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------
# Purpose:     Extract data from an ArcGIS Service, in chunks defined by
#              the service Max Record Count to get around that limitation.
#              Requires that JSON is supported by the service
#
# Author:      Grant Herbert
#
# Created:     12/11/2014
# Copyright:   (c) Grant Herbert 2014
# Licence:     MIT License
# -------------------------------------------------------------------------------
"""
This software is designed for use with ArcGIS as a toolbox tool.

This software is distributed with an MIT License.

THIS SOFTWARE IS SUPPLIED AS-IS, WITH NO WARRANTY OR GUARANTEE, EXPLICT OR IMPLICIT. THE AUTHORS
OF THIS SOFTWARE ASSUME NO LIABILITY FROM IMPROPER USE OR OPERATION.

"""

try:
    import sys
    import arcpy
    import urllib
    import urllib2
    import json
    import os
    import codecs
    import datetime
    import time
    from urlparse import urlparse
    from urlparse import urlunsplit
    import itertools
except ImportError, e:
    print e
    sys.exit()

# --------
# globals
arcpy.env.overwriteOutput = True
count_tries = 0
max_tries = 5
sleep_time = 2
# --------

def trace():
    import sys
    import traceback
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]  # script name + line number
    line = tbinfo.split(", ")[1]
    # Get Python syntax error
    synerror = traceback.format_exc().splitlines()[-1]
    return line, synerror


def output_msg(msg, severity=0):
    """ Adds a Message (in case this is run as a tool)
        and also prints the message to the screen (standard output)

        :param msg: text to output
        :param severity: 0 = none, 1 = warning, 2 = error
    """
    print msg

    # Split the message on \n first, so that if it's multiple lines,
    #  a GPMessage will be added for each line
    try:
        for string in msg.split('\n'):
            # Add appropriate geoprocessing message
            #
            if severity == 0:
                arcpy.AddMessage(string)
            elif severity == 1:
                arcpy.AddWarning(string)
            elif severity == 2:
                arcpy.AddError(string)
    except:
        pass


def test_url(token_url_test):
    try:
        if urllib2.urlopen(token_url_test):
            output_msg("successful url test: {}".format(token_url_test))
            return token_url_test
    except urllib2.HTTPError as e:
        if e.code == 404:
            output_msg("404 error: {}".format(token_url_test))
            return None
    except urllib2.URLError as e:
        return None


def gentoken(username, password, referer, expiration=240):
    """ Get access token.
        :param username: valid username
        :param password: valid password
        :param referer: valid referer url (eg "https://www.arcgis.com")
        :param expiration: optional validity time in minutes (default 240)
    """
    query_dict = {'username': username,
                  'password': password,
                  'expiration': str(expiration),
                  #'client': 'referer',
                  'client': 'requestip',
                  'referer': referer,
                  'f': 'json'}

    # check for ArcGIS token generator url
    tokenUrl = None
    token_url_array = [referer + r"/sharing/rest/generateToken",
                       referer + r"/arcgis/tokens/generateToken"]
    for url2test in token_url_array:
        if test_url(url2test):
            tokenUrl = url2test
            break
    if tokenUrl:
        tokenResponse = urllib2.urlopen(tokenUrl, urllib.urlencode(query_dict))
        token = json.loads(tokenResponse.read(), strict=False)
    else:
        token = {"error": "unable to get token"}
    return token


def get_data(query):
    """ :param query: url query string
        Download the data.
        Return a JSON object
        Automatically retries up to max_tries times.
    """
    global count_tries
    global max_tries
    global sleep_time


    try:
        response = urllib2.urlopen(query).read()  #get a byte str by default
        if response:
            try:
                response = response.decode('utf-8')  # convert to unicode
            except UnicodeDecodeError:
                response = response.decode('unicode-escape')  # convert to unicode
            # load to json and check for error
            resp_json = json.loads(response)
            if resp_json.get('error'):
                output_msg(resp_json['error'])
                return None
            else:
                return resp_json
        else:
            return None

    except Exception, e:
        output_msg(str(e), severity=1)
        # sleep and try again
        if hasattr(e, 'errno') and e.errno == 10054:
                #connection forcible closed, extra sleep pause
                time.sleep(sleep_time)
        time.sleep(sleep_time)
        count_tries += 1
        if count_tries > max_tries:
            count_tries = 0
            output_msg("Error: ACCESS_FAILED")
            return None
        else:
            output_msg("Attempt {0} of {1}".format(count_tries, max_tries))
            return get_data(query=query)


def combine_data(fc_list, output_fc):
    """
        :param fc_list: array of featureclass paths as strings
        :param output_fc: path to output dataset
        Combine the downloaded datafiles into one
        fastest approach is to use cursor
    """
    for fc in fc_list:
        if fc_list.index(fc) == 0:
            # append to first dataset. much faster
            output_msg("Prepping first dataset {0}".format(fc))
            if arcpy.Exists(output_fc):
                output_msg("{0} exists, deleting...".format(output_fc))
                arcpy.Delete_management(output_fc)
            arcpy.Rename_management(fc, output_fc) # rename the first dataset to the final name
            output_msg("Created {0}".format(output_fc))
            arcpy.CopyFeatures_management(output_fc, fc) # duplicate first one so delete later doesn't fail
            insertRows = arcpy.da.InsertCursor(output_fc, ["SHAPE@","*"])
        else:
            searchRows = arcpy.da.SearchCursor(fc, ["SHAPE@","*"]) # append to first dataset
            for search_row in searchRows:
                insertRows.insertRow(search_row)
            del search_row, searchRows
            output_msg("Appended {0}...".format(fc))
    del insertRows


def grouper(iterable, n, fillvalue=None):
    """ Cut iterable into n sized groups
        from itertools documentation, may not be most efficient, fillvalue causes issue
        :param iterable: object to iterate over
        :param n: int value to group
        :param fillvalue: value to fill with if chunk smaller than n
    """
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)
    # alternative without fillvalue to test
    # http://stackoverflow.com/questions/3992735/python-generator-that-groups-another-iterable-into-groups-of-n
    #return iter(lambda: list(IT.islice(iterable, n)), [])


def createLayerFile(service_info, service_name, layer_source, output_folder):
    """
    write out a layer file from service renderer information, providing
     a service name, a layer source and an output folder
    """
    try:
        render_info = {"drawingInfo": {"renderer": {}}}
        render_info["drawingInfo"]['renderer'] = service_info.get('drawingInfo').get('renderer')

        render_file = os.path.join(output_folder, service_name + "_renderer.txt")
        with open(render_file, 'w') as r_file:
            json.dump(render_info, r_file)
            output_msg("Yar! {0} Service renderer stashed in '{1}'".format(service_name, render_file))

        layer_file = os.path.join(output_folder, service_name + ".lyr")
        output_msg("Sketchin' yer layer, {}".format(layer_file))

        layer_temp = arcpy.MakeFeatureLayer_management(layer_source, service_name)
        arcpy.SaveToLayerFile_management(in_layer=layer_temp, out_layer=layer_file, is_relative_path="RELATIVE")
        lyr_update = arcpy.mapping.Layer(layer_file)
        lyr_update.updateLayerFromJSON(render_info)
        lyr_update.save()
        output_msg("Stashed yer layer, {}".format(layer_file))

    except Exception, e:
        output_msg(str(e), severity=1)
        output_msg("Failed yer layer file drawin'")


def extract_domain_info(service_info):
    """extract domain information from service info"""
    # TODO return json of domains?
    ## find fields array, loop through fields and find field name and domain not null, extract domain values from 'domain' codedValues array
    # "domain":{"type":"codedValue","name":"RampType","codedValues":[{"name":"Perpendicular","code":"Perpendicular"},{"name":"Diagonal","code":"Diagonal"},{"name":"Parallel","code":"Parallel"},{"name":"Combination","code":"Combination"},{"name":"Built Up","code":"Built Up"},{"name":"Depressed","code":"Depressed"},{"name":"Other","code":"Other"},{"name":"Unknown","code":"Unknown"}]}

##    fields = service_info.get('fields')
##    for field in fields:
##        domain = field[0].get('domain') # type, name, coded values or null
##        if domain:
##            dname = domain.get('name')
##            dtype = domain.get('type')
##            dcodedvalues = domain.get('codedValues')
    pass


def create_domains_from(domain_json):
    """create domains from json"""
    #TODO extract individual domains
    #pass each to appropriate domain writer
    pass


def authenticate(username, password, service_endpoint, referring_domain):
    # set referring domain if supplied
    # or try to infer it from url
    if referring_domain != '':
        if referring_domain[:5] == 'http:':
            refer = 'https' + referring_domain[4:]
        else:
            refer = referring_domain
    else:
        u = urlparse(service_endpoint)
        if u.netloc.find('arcgis.com') > -1:
            # is an esri domain
            refer = r"https://www.arcgis.com"
        else:
            # generate from service url and hope it works
            if u.scheme == 'http':
                # must be https for token
                refer = urlunsplit(['https', u.netloc, '', '', ''])
            else:
                refer = urlunsplit([u.scheme, u.netloc, '', '', ''])

    # set up authentication
    # http://stackoverflow.com/questions/1045886/https-log-in-with-urllib2
    passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
    # this creates a password manager
    passman.add_password(None, service_endpoint, username, password)
    # because we have put None at the start it will always
    # use this username/password combination for  urls
    # for which `theurl` is a super-url

    authhandler = urllib2.HTTPBasicAuthHandler(passman)
    # create the AuthHandler
    opener = urllib2.build_opener(authhandler)
    # user agent spoofing
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]

    urllib2.install_opener(opener)
    # All calls to urllib2.urlopen will now use our handler
    # Make sure not to include the protocol in with the URL, or
    # HTTPPasswordMgrWithDefaultRealm will be very confused.
    # You must (of course) use it when fetching the page though.
    # authentication is now handled automatically in urllib2.urlopen

    # generate a token
    tokenjson = gentoken(username=username, password=password, referer=refer)
    return tokenjson


#-------------------------------------------------
def main():
    global count_tries
    global max_tries
    global sleep_time

    start_time = datetime.datetime.today()

    try:
        # arcgis toolbox parameters
        service_endpoint = arcpy.GetParameterAsText(0) # Service endpoint required
        output_workspace = arcpy.GetParameterAsText(1) # gdb/folder to put the results required
        max_tries = arcpy.GetParameter(2) # max number of retries allowed required
        sleep_time = arcpy.GetParameter(3) # max number of retries allowed required`
        strict_mode = arcpy.GetParameter(4) # JSON check True/False required
        username = arcpy.GetParameterAsText(5)
        password = arcpy.GetParameterAsText(6)
        referring_domain = arcpy.GetParameterAsText(7) # auth domain
        existing_token = arcpy.GetParameterAsText(8) # valid token value
        query_str = arcpy.GetParameterAsText(9) # query string

        sanity_max_record_count = 10000

        # to query by geometry need [xmin,ymin,xmax,ymax], spatial reference, and geometryType (eg esriGeometryEnvelope

        if service_endpoint == '':
            output_msg("Avast! Can't plunder nothing from an empty url! Time to quit.")
            sys.exit()

        if not type(strict_mode) is bool:
            strict_mode = True

        if not type(max_tries) is int:
            max_tries = int(max_tries)

        if not type(sleep_time) is int:
           sleep_time = int(sleep_time)

        if query_str:
            query_str = urllib.quote(query_str)

        if output_workspace == '':
            output_workspace = os.getcwd()

        output_desc = arcpy.Describe(output_workspace)
        output_type = output_desc.dataType

        if output_type == "Folder": # To Folder
            output_folder = output_workspace
        else:
            output_folder = output_desc.path

        token = ''
        if username and not existing_token:
            tokenjson = authenticate(username, password, service_endpoint, referring_domain)
            if "token" in tokenjson:
                token = tokenjson['token']
            else:
                output_msg(
                    "Avast! The scurvy gatekeeper says 'Could not generate a token with the username and password provided'.",
                    severity=2)
                if "error" in tokenjson:
                    output_msg(tokenjson["error"], severity=2)
                elif "token" not in tokenjson:
                    output_msg(tokenjson['messages'], severity=2)
                raise ValueError("Token Error")
        elif existing_token:
            token = existing_token
        else:
            # build a generic opener with the use agent spoofed
            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]
            urllib2.install_opener(opener)
        tokenstring = ''
        if len(token) > 0:
            tokenstring = '&token=' + token

        output_msg("Start the plunder! {0}".format(service_endpoint))
        output_msg("We be stashing the booty in {0}".format(output_workspace))

        service_call = urllib2.urlopen(service_endpoint + '?f=json' + tokenstring).read()
        if service_call and (service_call.find('error') == -1):
            service_layer_info = json.loads(service_call, strict=False)
        else:
            raise Exception("'service_call' failed to access {0}".format(service_endpoint))
        service_version = service_layer_info.get('currentVersion')

        service_layers_to_walk = []
        service_layers_to_get = []

        # catch root or group layers url entered
        # TODO walk folders (ignore 'utilities'?)
        if 'folders' in service_layer_info.keys() and len(service_layer_info.get('folders')) > 0:
            catalog_folder = service_layer_info.get('folders')
            for folder_name in catalog_folder:
                pass

        # get list of service urls to walk
        if 'services' in service_layer_info.keys() and len(service_layer_info.get('services')) > 0:
            catalog_services = service_layer_info.get('services')
            for service in catalog_services:
                servicetype = service['type']
                servicename = service['name']
                folder, sname = servicename.split('/')
                if servicetype in ['MapServer', 'FeatureServer']:
                    if service_endpoint.endswith(folder):
                        service_url = service_endpoint + '/' + sname + '/' + servicetype
                    else:
                        service_url = service_endpoint + '/' + servicename + '/' + servicetype
                    service_layers_to_walk.append(service_url)

        if len(service_layers_to_walk) == 0:
            # no services or folders
            service_layers_to_walk.append(service_endpoint)

        ##service_type = service_layer_info.get('type') # change at 10.4.1 type = "Group Layer"
        # if catalog_services:
        #    raise ValueError("Unable to pillage a service root url at this time. Enter a FeatureServer layer url!")

        for url in service_layers_to_walk:
            # go get the json and information and walk down until you get all the service urls
            service_call = json.load(urllib2.urlopen(url + '?f=json' + tokenstring))

            # for getting all the layers, start with a list of sublayers
            service_layers = None
            service_layer_type = None
            if service_call.get('layers'):
                service_layers = service_call.get('layers')
                service_layer_type = 'layers'
            elif service_call.get('subLayers'):
                service_layers = service_layer_info.get('subLayers')
                service_layer_type = 'sublayers'

            # subLayers an array of objects, each has an id
            if service_layers is not None:
                # has sub layers, get em all
                for lyr in service_layers:
                    # service_call.get('type') in ['MapServer', 'FeatureServer']
                    if not lyr.get('subLayerIds'):  # ignore group layers
                        lyr_id = lyr.get('id')
                        if service_layer_type == 'layers':
                            sub_layer_url = url + '/' + str(lyr_id)
                            # add the full url
                            service_layers_to_get.append(sub_layer_url)
                        elif service_layer_type == 'sublayers':
                            # handled differently, drop the last section and use id
                            sub_endpoint = url.rsplit('/', 1)
                            service_layers_to_get.append(sub_layer_url)
            else:
                # no sub layers
                # check if group layer
                if service_call.get('type'):
                    if not service_call.get('type') in ("Group Layer", "Raster Layer"):
                        service_layers_to_get.append(url)

        for lyr in service_layers_to_get:
            output_msg('Found {0}'.format(lyr))

        for slyr in service_layers_to_get:
            count_tries = 0
            out_shapefile_list = [] # for file merging.
            response = None
            current_iter = 0
            max_record_count = 0
            feature_count = 0
            final_geofile = ''

            output_msg("Now pillagin' yer data from {0}".format(slyr))
            if slyr == service_endpoint: # no need to get it again
                service_info = service_layer_info
            else:
                service_info_call = urllib2.urlopen(slyr + '?f=json' + tokenstring).read()
                if service_info_call:
                    service_info = json.loads(service_info_call, strict=False)
                else:
                    raise Exception("'service_info_call' failed to access {0}".format(slyr))

            if not service_info.get('error') and not service_info.get('type') in ("Raster Layer"):
                # add url to info
                service_info[u'serviceURL'] = slyr

                # get count
                if query_str == '':
                    feature_count_call = urllib2.urlopen(slyr + '/query?where=1%3D1&returnCountOnly=true&f=pjson' + tokenstring).read()
                else:
                    feature_count_call = urllib2.urlopen(slyr + '/query?where=' + query_str + '&returnCountOnly=true&f=pjson' + tokenstring).read()

                if feature_count_call:
                    feature_count = json.loads(feature_count_call)
                    service_info[u'FeatureCount'] = feature_count.get('count')

                service_name = service_info.get('name')
                # clean up the service name (remove invalid characters)
                service_name_cl = service_name.encode('ascii', 'ignore') # strip any non-ascii characters that may cause an issue
                service_name_cl = arcpy.ValidateTableName(service_name_cl, output_workspace) # remove any other problematic characters
                ##output_msg("'{0}' will be stashed as '{1}'".format(service_name, service_name_cl))
                info_filename = service_name_cl + "_info.txt"
                info_file = os.path.join(output_folder, info_filename)

                # write out the service info for reference
                with open(info_file, 'w') as i_file:
                    json.dump(service_info, i_file, sort_keys=True, indent=4, separators=(',', ': '))
                    output_msg("Yar! {0} Service info stashed in '{1}'".format(service_name, info_file))

                # TODO extract domains
                domain_json = extract_domain_info(service_info)
                # turn domain info into domains or table depending on output
                create_domains_from(domain_json)

                if strict_mode:
                    # check JSON supported
                    supports_json = False
                    if 'supportedQueryFormats' in service_info:
                        supported_formats = service_info.get('supportedQueryFormats').split(",")
                        for data_format in supported_formats:
                            if data_format == "JSON":
                                supports_json = True
                                break
                    else:
                        output_msg('Unable to check supported formats. Check {0} for details'.format(info_file))
                else:
                    # assume JSON supported
                    supports_json = True

                if supports_json:
                    try:
                        # loop through fields in service_info, get objectID field
                        objectid_field = "OBJECTID"
                        if 'fields' in service_info:
                            field_list = service_info.get('fields')
                            if field_list:
                                for field in field_list:
                                    if field.get('type') == 'esriFieldTypeOID':
                                        objectid_field = field.get('name')
                                        break
                        else:
                            output_msg("No field list returned - forging ahead with {0}".format(objectid_field))

                        if query_str =='':
                            feat_OIDLIST_query = r"/query?where=" + objectid_field + r"+%3E+0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=&returnGeometry=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=true&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json" + tokenstring
                        else:
                            feat_OIDLIST_query = r"/query?where=" + query_str + r"&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=&returnGeometry=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=true&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json" + tokenstring
                        # to query using geometry,&geometry=   &geometryType= esriGeometryEnvelope &inSR= and probably spatial relationship and buffering
                        feat_query = r"/query?objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json" + tokenstring

                        max_record_count = service_info.get('maxRecordCount') # maximum number of records returned by service at once

                        if max_record_count > sanity_max_record_count:
                            output_msg(
                                "{0} max records is a wee bit large, using {1} instead...".format(max_record_count,
                                                                                                  sanity_max_record_count))
                            max_record_count = sanity_max_record_count

                        # extract using actual OID values is the safest way
                        feature_OIDs = None
                        feature_query = json.loads(urllib2.urlopen(slyr + feat_OIDLIST_query).read())
                        if feature_query and 'objectIds' in feature_query:
                            feature_OIDs = feature_query["objectIds"]
                        else:
                            output_msg('Unable to get OID values: {}'.format(feature_query))

                        if feature_OIDs:
                            OID_count = len(feature_OIDs)
                            sortie_count = OID_count//max_record_count + (OID_count % max_record_count > 0)
                            output_msg("{0} records, in chunks of {1}, err, that be {2} sorties. Ready lads!".format(OID_count, max_record_count, sortie_count))

                            feature_OIDs.sort()
                            # chunk them
                            for group in grouper(feature_OIDs, max_record_count):
                                # reset count_tries
                                count_tries = 0
                                start_oid = group[0]
                                end_oid = group[max_record_count-1]
                                if end_oid is None: # reached the end of the iterables
                                    # loop through and find last oid
                                    # need this due to fillvalue of None in grouper
                                    for i in reversed(group):
                                        if i is not None:
                                            end_oid = i
                                            break

                                # >= %3E%3D, <= %3C%3D
                                if query_str == '':
                                    where_clause = "&where={0}+%3E%3D+{1}+AND+{2}+%3C%3D+{3}".format(objectid_field, str(start_oid), objectid_field, str(end_oid))
                                else:
                                    where_clause = "&where={0}+AND+{1}+%3E%3D+{2}+AND+{3}+%3C%3D+{4}".format(query_str, objectid_field,
                                                                                                     str(start_oid),
                                                                                                     objectid_field,
                                                                                                     str(end_oid))
                                # response is a string of json with the attr and geom
                                query = slyr + feat_query + where_clause
                                response = get_data(query) # expects json object. An error will return none
                                if not response or not response.get('features'):
                                    raise ValueError("Abandon ship! Data access failed! Check what ye manag'd to plunder before failure.")
                                else:
                                    feature_dict = response["features"] # load the features so we can check they are not empty

                                    if len(feature_dict) != 0:
                                        # convert response to json file on disk then to shapefile (is fast)
                                        out_JSON_name = service_name_cl + "_" + str(current_iter) + ".json"
                                        out_JSON_file = os.path.join(output_folder, out_JSON_name)

                                        #with open(out_JSON_file, 'w') as out_file:
                                        #    out_file.write(response.encode('utf-8')) #back from unicode
                                        with codecs.open(out_JSON_file, 'w', 'utf-8') as out_file:
                                            data = json.dumps(response, ensure_ascii=False)
                                            out_file.write(data)

                                        output_msg("Nabbed some json data fer ye: '{0}', oids {1} to {2}".format(out_JSON_name, start_oid, end_oid))

                                        if output_type == "Folder":
                                            out_file_name = service_name_cl + "_" + str(current_iter) + ".shp"
                                        else:
                                            out_file_name = service_name_cl + "_" + str(current_iter)

                                        out_geofile = os.path.join(output_workspace, out_file_name)

                                        output_msg("Converting json to {0}".format(out_geofile))
                                        arcpy.JSONToFeatures_conversion(out_JSON_file, out_geofile)
                                        out_shapefile_list.append(out_geofile)
                                        os.remove(out_JSON_file) # clean up the JSON file

                                    current_iter += max_record_count

                        else:
                            # no objectids
                            output_msg("No feature IDs found!")
                            raise ValueError("Aaar, plunderin' failed")

                        # download complete, create a final output
                        if output_type == "Folder":
                            final_geofile = os.path.join(output_workspace, service_name_cl + ".shp")
                        else:
                            final_geofile = os.path.join(output_workspace, service_name_cl)

                        output_msg("Stashin' all the booty in '{0}'".format(final_geofile))

                        #combine all the data
                        combine_data(fc_list=out_shapefile_list, output_fc=final_geofile)

                        createLayerFile(service_info=service_info, service_name=service_name, layer_source=final_geofile, output_folder=output_folder)

                        end_time = datetime.datetime.today()
                        elapsed_time = end_time - start_time
                        output_msg("{0} plundered in {1}".format(final_geofile, str(elapsed_time)))

                    except ValueError, e:
                        output_msg("ERROR: " + str(e), severity=2)

                    except Exception, e:
                        line, err = trace()
                        output_msg("Script Error\n{0}\n on {1}".format(err, line), severity=2)
                        output_msg(arcpy.GetMessages())

                    finally:
                        if arcpy.Exists(final_geofile):
                            data_count = int(arcpy.GetCount_management(final_geofile)[0])
                            if data_count == OID_count: #we got it all
                                output_msg("Scrubbing the decks...")
                                for fc in out_shapefile_list:
                                    arcpy.Delete_management(fc)
                            else:
                                output_msg("Splicin' the data failed - found {0} but expected {1}. Check {2} to see what went wrong.".format(data_count, OID_count, final_geofile))

                else:
                    # no JSON output
                    output_msg("Aaaar, ye service does not support JSON output. Can't do it.")
            else:
                if service_info.get('error'):
                    # service info error
                    output_msg("Error: {0}".format(service_info.get('error')), severity=2)
                else:
                    output_msg('Layer skipped')

    except ValueError, e:
        output_msg("ERROR: " + str(e), severity=2)

    except Exception, e:
        if hasattr(e, 'errno') and e.errno == 10054:
            output_msg("ERROR: " + str(e), severity=2)
        else:
            line, err = trace()
            output_msg("Error\n{0}\n on {1}".format(err, line), severity=2)
        output_msg(arcpy.GetMessages())

    finally:
        end_time = datetime.datetime.today()
        elapsed_time = end_time - start_time
        output_msg("Plunderin' done, in " + str(elapsed_time))

if __name__ == '__main__':
    main()
