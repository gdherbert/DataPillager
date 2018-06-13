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
    import re
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
            if severity == 0:
                arcpy.AddMessage(string)
            elif severity == 1:
                arcpy.AddWarning(string)
            elif severity == 2:
                arcpy.AddError(string)
    except:
        pass


def test_url(url_to_test):
    """test a url for validity (non-404)
    :param token_url: String
    """
    try:
        if urllib2.urlopen(url_to_test):
            output_msg("Ho, a successful url test: {}".format(url_to_test))
            return url_to_test
    except urllib2.HTTPError as e:
        if e.code == 404:
            output_msg("Arr, 404 error: {}".format(url_to_test))
            return None
    except urllib2.URLError as e:
        return None


def get_adapter_name(url_string):
    """extract web adaptor name from endpoint
    :param url_string: url of service
    """
    u = urlparse(url_string)
    if u.netloc.find('arcgis.com') > -1:
        # is an esri domain
        refer = r"https://www.arcgis.com"
        adapter_name = u.path.split("/")[2]  # third element
    else:
        adapter_name = u.path.split("/")[1] # second element
    return adapter_name


def get_referring_domain(url_string):
    """get referring domain part of url
    :param url_string url of service
    """
    u = urlparse(url_string)
    if u.netloc.find('arcgis.com') > -1:
        # is an esri domain
        ref_domain = r"https://www.arcgis.com"
    else:
        # generate from service url and hope it works
        if u.scheme == 'http':
            ref_domain = urlunsplit(['https', u.netloc, '', '', ''])
        else:
            ref_domain = urlunsplit([u.scheme, u.netloc, '', '', ''])
    return ref_domain


def get_token(username, password, referer, adapter_name, client_type='requestip', expiration=240):
    """ Get Esri access token. Uses requestip by default
        :param username: valid username
        :param password: valid password
        :param referer: referer url
        :param adapter_name: name of the arcgis server adapter
        :param client_type: whether to use referer value over requestip (default False uses requestip)
        :param expiration: optional validity time in minutes (default 240)
    """
    query_dict = {'username': username,
                  'password': password,
                  'expiration': str(expiration),
                  'client': client_type,
                  'referer': referer,
                  'f': 'json'}

    # check for ArcGIS token generator url
    token_url = None
    token_url_array = [referer + r"/sharing/rest/generateToken",
                       referer + r"/" + adapter_name + r"/tokens/generateToken"]
    for url2test in token_url_array:
        if test_url(url2test):
            token_url = url2test
            break
    if token_url:
        token_response = urllib2.urlopen(token_url, urllib.urlencode(query_dict))
        token_json = json.loads(token_response.read(), strict=False)
    else:
        token_json = {"error": "unable to get token"}

    if "token" in token_json:
        token = token_json['token']
        return token
    else:
        output_msg(
            "Avast! The scurvy gatekeeper says 'Could not generate a token with the username and password provided'.",
            severity=2)
        if "error" in token_json:
            output_msg(token_json["error"], severity=2)
        elif "message" in token_json:
            output_msg(token_json['message'], severity=2)
        raise ValueError("Token Error")


def get_all_the_layers(service_endpoint, tokenstring):
    """walk the endpoint and extract feature layer or map layer urls
    :param service_endpoint starting url
    :param tokenstring string containing token for authentication
    """
    service_call = urllib2.urlopen(service_endpoint + '?f=json' + tokenstring).read()
    if service_call:
        service_layer_info = json.loads(service_call, strict=False)
        if service_layer_info.get('error'):
            raise Exception("Gaaar, 'service_call' failed to access {0}".format(service_endpoint))
    else:
        raise Exception("Gaaar, 'service_call' failed to access {0}".format(service_endpoint))

    service_version = service_layer_info.get('currentVersion')

    service_layers_to_walk = []
    service_layers_to_get = []

    # search any folders
    if 'folders' in service_layer_info.keys() and len(service_layer_info.get('folders')) > 0:
        catalog_folder = service_layer_info.get('folders')
        folder_list = [f for f in catalog_folder if f.lower() not in 'utilities']
        for folder_name in folder_list:
            output_msg("Ahoy, I be searching {} for hidden treasure...".format(folder_name), severity=0)
            lyr_list = get_all_the_layers(service_endpoint + '/' + folder_name, tokenstring)
            if lyr_list:
                service_layers_to_walk.extend(lyr_list)

    # get list of service urls
    if 'services' in service_layer_info.keys() and len(service_layer_info.get('services')) > 0:
        catalog_services = service_layer_info.get('services')
        for service in catalog_services:
            servicetype = service['type']
            servicename = service['name']
            if servicetype in ['MapServer', 'FeatureServer']:
                service_url = service_endpoint + '/' + servicename + '/' + servicetype
                if servicename.find('/') > -1:
                    folder, sname = servicename.split('/')
                    if service_endpoint.endswith(folder):
                        service_url = service_endpoint + '/' + sname + '/' + servicetype
                
                service_layers_to_walk.append(service_url)

    if len(service_layers_to_walk) == 0:
        # no services or folders
        service_layers_to_walk.append(service_endpoint)

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
                if not lyr.get('subLayerIds'):  # ignore group layers
                    lyr_id = str(lyr.get('id'))
                    if service_layer_type == 'layers':
                        sub_layer_url = url + '/' + lyr_id
                        lyr_list = get_all_the_layers(sub_layer_url, tokenstring)
                        if lyrlist:
                            service_layers_to_walk.extend(lyr_list)
                        # add the full url
                        else:
                            service_layers_to_get.append(sub_layer_url)
                    elif service_layer_type == 'sublayers':
                        # handled differently, drop the parent layer id and use sublayer id
                        sub_endpoint = url.rsplit('/', 1)[0]
                        sub_layer_url = sub_endpoint + '/' + lyr_id
                        lyr_list = get_all_the_layers(sub_layer_url, tokenstring)
                        if lyr_list:
                            service_layers_to_walk.extend(lyr_list)
                        else:
                            service_layers_to_get.append(sub_layer_url)
        else:
            # no sub layers
            # check if group layer
            if service_call.get('type'):
                if not service_call.get('type') in ("Group Layer", "Raster Layer"):
                    service_layers_to_get.append(url)

    return service_layers_to_get


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
            return resp_json
        else:
            return {'error': 'no response received'}

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
            output_msg("Avast! Error: ACCESS_FAILED")
            return None
        else:
            output_msg("Hold fast, attempt {0} of {1}".format(count_tries, max_tries))
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
            output_msg("Prepping yer first dataset {0}".format(fc))
            if arcpy.Exists(output_fc):
                output_msg("Avast! {0} exists, deleting...".format(output_fc), severity=1)
                arcpy.Delete_management(output_fc)
            arcpy.Rename_management(fc, output_fc) # rename the first dataset to the final name
            output_msg("Created {0}".format(output_fc))
            arcpy.CopyFeatures_management(output_fc, fc) # duplicate first one so delete later doesn't fail
            insert_rows = arcpy.da.InsertCursor(output_fc, ["SHAPE@","*"])
        else:
            search_rows = arcpy.da.SearchCursor(fc, ["SHAPE@","*"]) # append to first dataset
            for row in search_rows:
                insert_rows.insertRow(row)
            del row, search_rows
            output_msg("Appended {0}...".format(fc))
    del insert_rows


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


def create_layer_file(service_info, service_name, layer_source, output_folder):
    """
    write out a layer file from service renderer information, providing
    :param service_info: json (to extract the drawingInfo from)
    :param service_name: String
    :param layer_source: String path to file
    :param output_folder: String path
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

def make_service_name(service_info, output_workspace, output_folder_len):
    global service_output_name_tracking_list
    global output_type

    # establish a unique name that isn't too long
    # TODO 160 character limit for filegeodatabase
    max_str_len = 230  # sanity length for windows systems
    if output_type == 'Workspace':
        max_name_len = 150  # based on fgdb
    else:
        max_name_len = max_str_len - output_folder_len
    
    parent_name = ''
    parent_id = ''
    service_name = service_info.get('name')
    service_id = str(service_info.get('id'))

    # clean up the service name (remove invalid characters)
    service_name_cl = service_name.encode('ascii', 'ignore')  # strip any non-ascii characters that may cause an issue
    # remove multiple underscores and any other problematic characters
    service_name_cl = re.sub(r'[_]+', '_', arcpy.ValidateTableName(service_name_cl, output_workspace))
    service_name_cl = service_name_cl.rstrip('_')

    if len(service_name_cl) > max_name_len:
        service_name_cl = service_name_cl[:max_name_len]

    service_name_len = len(service_name_cl)

    if service_info.get('parentLayer'):
        parent_name = service_info.get('parentLayer').get('name')
        parent_id = str(service_info.get('parentLayer').get('id'))

    if output_folder_len  + service_name_len > max_str_len: # can be written to disc
        # shorten the service name
        max_len = max_str_len - output_folder_len
        if max_len < service_name_len:
            service_name_cl = service_name_cl[:max_len]

    # check if name already exists
    if service_name_cl not in service_output_name_tracking_list:
        service_output_name_tracking_list.append(service_name_cl)
    else:
        if service_name_cl + "_" + service_id not in service_output_name_tracking_list:
            service_name_cl += "_" + service_id
            service_output_name_tracking_list.append(service_name_cl)
        else:
            service_name_cl += parent_id + "_" + service_id

    return service_name_cl


#-------------------------------------------------
def main():
    global count_tries
    global max_tries
    global sleep_time
    global service_output_name_tracking_list
    global output_type
    
    start_time = datetime.datetime.today()

    try:
        # arcgis toolbox parameters
        service_endpoint = arcpy.GetParameterAsText(0) # String - URL of Service endpoint required
        output_workspace = arcpy.GetParameterAsText(1) # String - gdb/folder to put the results required
        max_tries = arcpy.GetParameter(2) # Int - max number of retries allowed required
        sleep_time = arcpy.GetParameter(3) # Int - max number of retries allowed required`
        strict_mode = arcpy.GetParameter(4) # Bool - JSON check True/False required
        username = arcpy.GetParameterAsText(5) # String - username optional
        password = arcpy.GetParameterAsText(6) # String - password optional
        referring_domain = arcpy.GetParameterAsText(7) # String - url of auth domain
        existing_token = arcpy.GetParameterAsText(8) # String - valid token value
        query_str = arcpy.GetParameterAsText(9) # String - valid SQL query string

        sanity_max_record_count = 10000

        # to query by geometry need [xmin,ymin,xmax,ymax], spatial reference, and geometryType (eg esriGeometryEnvelope
        service_output_name_tracking_list = []

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

        adapter_name = get_adapter_name(service_endpoint)
        token_client_type = 'requestip'
        if referring_domain != '':
            referring_domain = referring_domain.replace('http:', 'https:')
            token_client_type = 'referer'
        else:
            referring_domain = get_referring_domain(service_endpoint)
            if referring_domain == r"https://www.arcgis.com":
                token_client_type = 'referer'

        # build a generic opener with the use agent spoofed
        opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', 'Mozilla/5.0')]
        urllib2.install_opener(opener)

        token = ''
        if username and not existing_token:
            token = get_token(username=username, password=password, referer=referring_domain, adapter_name=adapter_name,
                              client_type=token_client_type)
        elif existing_token:
            token = existing_token

        tokenstring = ''
        if len(token) > 0:
            tokenstring = '&token=' + token

        output_msg("Start the plunder! {0}".format(service_endpoint))
        output_msg("We be stashing the booty in {0}".format(output_workspace))

        service_layers_to_get = get_all_the_layers(service_endpoint, tokenstring)
        output_msg("Blimey, {} layers for the pillagin'".format(len(service_layers_to_get)))
        for slyr in service_layers_to_get:
            count_tries = 0
            out_shapefile_list = [] # for file merging.
            response = None
            current_iter = 0
            max_record_count = 0
            feature_count = 0
            final_geofile = ''

            output_msg("Now pillagin' yer data from {0}".format(slyr))
            service_info_call = urllib2.urlopen(slyr + '?f=json' + tokenstring).read()
            if service_info_call:
                service_info = json.loads(service_info_call, strict=False)
            else:
                raise Exception("'service_info_call' failed to access {0}".format(slyr))

            if not service_info.get('error'):
                # add url to info
                service_info[u'serviceURL'] = slyr

                # assume JSON supported
                supports_json = True
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
                        output_msg('Strict mode scuttled, no supported formats')

                objectid_field = "OBJECTID"
                if 'fields' in service_info:
                    field_list = service_info.get('fields')
                    if field_list:
                        for field in field_list:
                            ftype = field.get('type')
                            if ftype == 'esriFieldTypeOID':
                                objectid_field = field.get('name')
                else:
                    output_msg("No field list - come about using {0}!".format(objectid_field))

                # get count
                if query_str == '':
                    feature_count_call = urllib2.urlopen(slyr + '/query?where=1%3D1&returnCountOnly=true&f=pjson' + tokenstring).read()
                else:
                    feature_count_call = urllib2.urlopen(slyr + '/query?where=' + query_str + '&returnCountOnly=true&f=pjson' + tokenstring).read()

                if feature_count_call:
                    feature_count = json.loads(feature_count_call)
                    service_info[u'FeatureCount'] = feature_count.get('count')

                service_name_cl = make_service_name(service_info, output_workspace, len(output_folder))

                info_filename = service_name_cl + "_info.txt"
                info_file = os.path.join(output_folder, info_filename)

                # write out the service info for reference
                with open(info_file, 'w') as i_file:
                    json.dump(service_info, i_file, sort_keys=True, indent=4, separators=(',', ': '))
                    output_msg("Yar! {0} Service info stashed in '{1}'".format(service_name_cl, info_file))

                if supports_json:
                    try:
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
                            output_msg("Blast, no OID values: {}".format(feature_query))

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
                                    # loop through and find last oid, need this due to fillvalue of None in grouper
                                    for i in reversed(group):
                                        if i is not None:
                                            end_oid = i
                                            break

                                # >= %3E%3D, <= %3C%3D
                                if query_str == '':
                                    where_clause = "&where={0}+%3E%3D+{1}+AND+{2}+%3C%3D+{3}".format(objectid_field,
                                                                                                     str(start_oid),
                                                                                                     objectid_field,
                                                                                                     str(end_oid))
                                else:
                                    where_clause = "&where={0}+AND+{1}+%3E%3D+{2}+AND+{3}+%3C%3D+{4}".format(query_str,
                                                                                                             objectid_field,
                                                                                                             str(start_oid),
                                                                                                             objectid_field,
                                                                                                             str(end_oid))
                                # response is a string of json with the attributes and geometry
                                query = slyr + feat_query + where_clause
                                response = get_data(query) # expects json object
                                if not response.get('features'):
                                    raise ValueError("Abandon ship! Data access failed! Check what ye manag'd to plunder before failure.")
                                else:
                                    feature_dict = response["features"] # load the features so we can check they are not empty

                                    if len(feature_dict) != 0:
                                        # convert response to json file on disk then to gdb/shapefile (is fast)
                                        # can hit long filename issue!!!!
                                        # look at an arcpy.FeatureSet() to hold the data
                                        # some services produce JSON that errors a FeatureSet()
                                        ##fs = arcpy.FeatureSet()
                                        ##fs.load(response)

                                        out_JSON_name = service_name_cl + str(current_iter) + ".json"
                                        out_JSON_file = os.path.join(output_folder, out_JSON_name)
                                        with codecs.open(out_JSON_file, 'w', 'utf-8') as out_file:
                                            data = json.dumps(response, ensure_ascii=False)
                                            out_file.write(data)

                                        output_msg("Nabbed some json data fer ye: '{0}', oids {1} to {2}".format(out_JSON_name, start_oid, end_oid))

                                        if output_type == "Folder":
                                            out_file_name = service_name_cl + str(current_iter) + ".shp"
                                        else:
                                            out_file_name = service_name_cl + str(current_iter)
                                        out_geofile = os.path.join(output_workspace, out_file_name)

                                        output_msg("Converting yer json to {0}".format(out_geofile))
                                        # may not be needed if using a featureSet()
                                        arcpy.JSONToFeatures_conversion(out_JSON_file, out_geofile)
                                        ##arcpy.JSONToFeatures_conversion(fs, out_geofile)
                                        out_shapefile_list.append(out_geofile)
                                        os.remove(out_JSON_file) # clean up the JSON file

                                    current_iter += 1
                        else:
                            raise ValueError("Aaar, plunderin' failed")

                        # download complete, create a final output
                        if output_type == "Folder":
                            final_geofile = os.path.join(output_workspace, service_name_cl + ".shp")
                        else:
                            final_geofile = os.path.join(output_workspace, service_name_cl)

                        output_msg("Stashin' all the booty in '{0}'".format(final_geofile))

                        #combine all the data
                        combine_data(fc_list=out_shapefile_list, output_fc=final_geofile)

                        create_layer_file(service_info=service_info, service_name=service_name_cl, layer_source=final_geofile, output_folder=output_folder)

                        elapsed_time = datetime.datetime.today() - start_time
                        output_msg("{0} plundered in {1}".format(final_geofile, str(elapsed_time)))

                    except ValueError, e:
                        output_msg(str(e), severity=2)

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
                # service info error
                output_msg("Error: {0}".format(service_info.get('error')), severity=2)

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
        elapsed_time = datetime.datetime.today() - start_time
        output_msg("Plunderin' done, in " + str(elapsed_time))


if __name__ == '__main__':
    main()
