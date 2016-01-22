# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# Purpose:     Extract data from an ArcGIS Service, in chunks defined by
#              the service Max Record Count to get around that limitation.
#              Requires that JSON is supported by the service
#
# Author:      Grant Herbert
#
# Created:     12/11/2014
# Copyright:   (c) Grant Herbert 2014
# Licence:     MIT License
#-------------------------------------------------------------------------------
"""
THis software is designed for use with ArcGIS as a toolbox tool.

This software is distributed with an MIT License.

THIS SOFTWARE IS SUPPLIED AS-IS, WITH NO WARRANTY OR GARANTEE, EXPLICT OR IMPLICIT. THE AUTHORS
OF THIS SOFTWARE ASSUME NO LIABILITY FROM IMPROPER USE OR OPERATION.

"""

try:
    import sys
    import arcpy
    import urllib
    import urllib2
    import json
    import os
    import datetime
    import time
    from urlparse import urlparse
    from urlparse import urlunsplit
    import itertools
except ImportError, e:
    print e
    sys.exit()

#--------
# globals
arcpy.env.overwriteOutput = True
count_tries = 0
max_tries = 5
sleep_time = 2

#--------
def trace():
    import sys, traceback
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0] # script name + line number
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


def gentoken(username, password, referer, expiration=240):
    """ Get access token.
        :param username: valid username
        :param passowrd: valid password
        :param referer: valid referer url (eg "https://www.arcgis.com")
        :param expiration: optional validity time in minutes (default 240)
    """
    query_dict = {'username': username,
                  'password': password,
                  'expiration': str(expiration),
                  'client': 'referer',
                  'referer': referer,
                  'f': 'json'}

    query_string = urllib.urlencode(query_dict)
    # assume ArcGIS service has token generator at root
    tokenUrl = referer + r"/sharing/rest/generateToken"

    tokenResponse = urllib.urlopen(tokenUrl, urllib.urlencode(query_dict))
    token = json.loads(tokenResponse.read(), strict=False)

    if "error" in token:
        output_msg(token["error"], severity=2)
        return ""
    elif "token" not in token:
        output_msg(token['messages'], severity=2)
        return ""
    else:
        # Return the token to the function which called for it
        return token['token']


def get_data(query):
    """ Download the data.
        Returns a unicode string (utf-8)
        Automatically retries up to max_tries times.
    """
    global count_tries
    global max_tries
    global sleep_time
    response = None

    try:
        response = urllib2.urlopen(query).read() #get a byte str by default
        if response:
            try:
                response = response.decode('utf-8') # convert to unicode
            except UnicodeDecodeError:
                response = response.decode('unicode-escape') # convert to unicode
        return response

    except Exception, e:
        output_msg(str(e),severity=1)
        # sleep and try again
        if e.errno == 10054:
            #connection forcible closed, extra sleep pause
            time.sleep(sleep_time)
        time.sleep(sleep_time)
        count_tries += 1
        if count_tries > max_tries:
            count_tries = 0
            return u"ACCESS_FAILED"
        else:
            output_msg("Attempt {0} of {1}".format(count_tries, max_tries))
            return get_data(query)


def combine_data(fc_list, output_fc):
    """ Combine the downloaded datafiles into one
        fastest approach is to use cursor
    """
    for fc in fc_list:
        if fc_list.index(fc) == 0:
            ##arcpy.CopyFeatures_management(fc, outputFC) # adds OBJECTID if in gdb, special OID field fails to append
            # alternative - append to first dataset. much faster
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


#-------------------------------------------------
def main():
    global count_tries
    global max_tries
    global sleep_time

    start_time = datetime.datetime.today()

    try:
        # arcgis toolbox parameters
        service_endpoint = arcpy.GetParameterAsText(0) # Service endpoint
        output_workspace = arcpy.GetParameterAsText(1) # folder to put the results
        max_tries = arcpy.GetParameter(2) # max number of retries allowed
        sleep_time = arcpy.GetParameter(3) # max number of retries allowed
        username = arcpy.GetParameterAsText(4)
        password = arcpy.GetParameterAsText(5)
        referring_domain = arcpy.GetParameterAsText(6) # auth domain
        existing_token = arcpy.GetParameterAsText(7) # valid token value

        # to query by geometry need [xmin,ymin,xmax,ymax], spatial reference, and geometryType (eg esriGeometryEnvelope

        if service_endpoint == '':
            output_msg("Avast! Can't plunder nothing from an empty url! Time to quit.")
            sys.exit()

        if not type(max_tries) is int: # set default
            max_tries = int(max_tries)

        if not type(sleep_time) is int:
           sleep_time = int(sleep_time)

        if not existing_token:
            token = ''
        else:
            token = existing_token

        if output_workspace == '':
            output_workspace = os.getcwd()

        output_desc = arcpy.Describe(output_workspace)
        output_type = output_desc.dataType

        if output_type == "Folder": # To Folder
            output_folder = output_workspace
        else:
            output_folder = output_desc.path

        if username:
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

            # need to generate a new token
            token = gentoken(username, password, refer)
        else:
            #build a generic opener with the use agent spoofed
            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', 'Mozilla/5.0')]
            urllib2.install_opener(opener)

        if username and (token == ""):
            output_msg("Avast! The scurvy gatekeeper says 'Could not generate a token with the username and password provided'.", severity=2)

        else:
            output_msg("Start the plunder! {0}".format(service_endpoint))
            output_msg("We be stashing the booty in {0}".format(output_workspace))

            service_layers_to_get = []
            # other variables, calculated from the service
            service_call = urllib2.urlopen(service_endpoint + '?f=json&token=' + token).read()
            if service_call:
                service_layer_info = json.loads(service_call, strict=False)
            else:
                raise Exception("'service_call' failed to access {0}".format(service_endpoint))

            # for getting all the layers
            service_layers = service_layer_info.get('layers')
            if service_layers is not None:
                # has sub layers, get em all
                for lyr in service_layers:
                    if not lyr.get('subLayerIds'):
                        lyr_id = lyr.get('id')
                        service_layers_to_get.append(service_endpoint + '/' + str(lyr_id))
            else:
                # no sub layers
                service_layers_to_get.append(service_endpoint)
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
                    service_info_call = urllib2.urlopen(slyr + '?f=json&token=' + token).read()
                    if service_info_call:
                        service_info = json.loads(service_info_call, strict=False)
                    else:
                        raise Exception("'service_info_call' failed to access {0}".format(slyr))

                if not service_info.get('error'):
                    service_name = service_info.get('name')

                    # clean up the service name (remove invalid characters)
                    service_name_cl = service_name.encode('ascii', 'ignore') # strip any non-ascii characters that may cause an issue
                    service_name_cl = arcpy.ValidateTableName(service_name) # remove any other problematic characters
                    output_msg("'{0}' will be stashed as '{1}'".format(service_name, service_name_cl))

                    # write out the service info for reference
                    info_filename = service_name_cl + "_info.txt"
                    info_file = os.path.join(output_folder, info_filename)
                    with open(info_file, 'w') as f:
                        json.dump(service_info, f, sort_keys=True, indent=4, separators=(',', ': '))
                        output_msg("Yar! Service info stashed: {0}".format(info_file))

                    supports_json = False
                    if 'supportedQueryFormats' in service_info:
                        supported_formats = service_info.get('supportedQueryFormats').split(",")
                        for data_format in supported_formats:
                            if data_format == "JSON":
                                supports_json = True
                                break
                    else:
                        output_msg('Unable to check supported formats')

                    if supports_json == True:
                        try:
                            # loop through fields in service_info, get objectID field
                            objectid_field = "OBJECTID"
                            if 'fields' in service_info:
                                field_list = service_info.get('fields')
                                for field in field_list:
                                    if field.get('type') == 'esriFieldTypeOID':
                                        objectid_field = field.get('name')
                                        break
                            else:
                                output_msg("No field list returned - forging ahead with {0}".format(objectid_field))

                            # to query using geometry,&geometry=   &geometryType= esriGeometryEnvelope &inSR= and probably spatial relationship and buffering

                            feat_count_query = r"/query?where=" + objectid_field + r"+%3E+0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=&returnGeometry=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=true&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json&token=" + token
                            feat_OIDLIST_query = r"/query?where=" + objectid_field + r"+%3E+0&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=&returnGeometry=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=true&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json&token=" + token
                            feat_query = r"/query?objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=*&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json&token=" + token

                            max_record_count = service_info.get('maxRecordCount') # maximum number of records returned by service at once
                            feature_count = json.loads(urllib2.urlopen(slyr + feat_count_query).read())["count"]
                            sortie_count = feature_count//max_record_count + (feature_count % max_record_count > 0)
                            output_msg("{0} records, in chunks of {1}, err, that be {2} sorties".format(feature_count, max_record_count, sortie_count))

                            # extract using actual OID values is the safest way
                            feature_OIDs = json.loads(urllib2.urlopen(slyr + feat_OIDLIST_query).read())["objectIds"]

                            if feature_OIDs:
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
                                    where_clause = "&where={0}+%3E%3D+{1}+AND+{2}+%3C%3D+{3}".format(objectid_field, str(start_oid), objectid_field, str(end_oid))
                                    # response is a string of json with the attr and geom
                                    query = slyr + feat_query + where_clause
                                    response = get_data(query) # expects unicode
                                    if not response or (response == 'ACCESS_FAILED'):
                                        # break out
                                        raise ValueError("Abandon ship! Data access failed! Check what ye manag'd to plunder before failure.")
                                    else:
                                        feature_dict = json.loads(response, strict=False)["features"] # load the features so we can check they are not empty

                                        if len(feature_dict) != 0:
                                            # convert response to json file on disk then to shapefile (is fast)
                                            out_JSON_name = service_name_cl + "_" + str(current_iter) + ".json"
                                            out_JSON_file = os.path.join(output_folder, out_JSON_name)

                                            # in-memory version
                                            ##temp_output = "in_memory\\"
                                            ##out_file_name = service_name_cl + "_" + str(current_iter)
                                            ##out_geofile = os.path.join(temp_output, out_file_name)

                                            if output_type == "Folder":
                                                out_file_name = service_name_cl + "_" + str(current_iter) + ".shp"
                                            else:
                                                out_file_name = service_name_cl + "_" + str(current_iter)

                                            out_geofile = os.path.join(output_workspace, out_file_name)

                                            with open(out_JSON_file, 'w') as out_file:
                                                out_file.write(response.encode('utf-8')) #back from unicode

                                            # write temp output
                                            output_msg("Nabbed some data fer ye: '{0}', oids {1} to {2}".format(out_file_name, start_oid, end_oid))

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
                            combine_data(out_shapefile_list, final_geofile)

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
                                if data_count == feature_count: #we got it all
                                    output_msg("Scrubbing the decks...")
                                    for fc in out_shapefile_list:
                                        arcpy.Delete_management(fc)
                                else:
                                    output_msg("Splicin' the data failed - found {0} but expected {1}. Check {2} to see what went wrong.".format(data_count, feature_count, final_geofile))

                    else:
                        # no JSON output
                        output_msg("Aaaar, ye service does not support JSON output. Can't do it.")
                else:
                    # service info error
                    output_msg("Error: {0}".format(service_info.get('error')))

    except ValueError, e:
        output_msg("ERROR: " + str(e), severity=2)

    except Exception, e:
        if e.errno == 10054:
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
