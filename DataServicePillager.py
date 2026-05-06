# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------
# Purpose:     Extract data from an ArcGIS Service, in chunks defined by
#              the service Max Record Count to get around that limitation.
#              Requires that JSON is supported by the service

# Author:      Grant Herbert
#
# Created:     12/11/2014
# Copyright:   (c) Grant Herbert 2014
# Updated:    2024-06-20
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
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    from urllib3.exceptions import InsecureRequestWarning
    import urllib.parse
    import json
    import os
    import codecs
    import datetime
    import time
    import itertools
    import re
    import warnings
except ImportError as e:
    print(e)
    sys.exit()

# --------
# globals
arcpy.env.overwriteOutput = True
count_tries = 1
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
    print(msg)
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
        response = requests.get(url_to_test, timeout=10)  # Add timeout for reliability
        if response.status_code == 200:
            output_msg(f"Ho, a successful url test: {url_to_test}")
            return url_to_test
    except requests.RequestException:
        pass
    return None


def get_adapter_name(url_string):
    """extract web adaptor name from endpoint
    :param url_string: url of service
    """
    u = urllib.parse.urlparse(url_string)
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
    u = urllib.parse.urlparse(url_string)
    if u.netloc.find('arcgis.com') > -1:
        # is an esri domain
        ref_domain = r"https://www.arcgis.com"
    else:
        # generate from service url and hope it works
        if u.scheme == 'http':
            ref_domain = urllib.parse.urlunsplit(['https', u.netloc, '', '', ''])
        else:
            ref_domain = urllib.parse.urlunsplit([u.scheme, u.netloc, '', '', ''])
    return ref_domain


def get_token(username, password, referer, adapter_name, client_type='requestip', expiration=240, session=None):
    """ Get Esri access token. Uses requestip by default
        :param username: valid username
        :param password: valid password
        :param referer: referer url
        :param adapter_name: name of the arcgis server adapter
        :param client_type: whether to use referer value over requestip (default False uses requestip)
        :param expiration: optional validity time in minutes (default 240)
        :param session: requests.Session for consistency
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
        if session:
            response = session.post(token_url, data=query_dict)
        else:
            response = requests.post(token_url, data=query_dict)
        token_json = response.json()
    else:
        token_json = {"error": "unable to get token"}

    if "token" in token_json:
        token = token_json['token']
        return token
    else:
        output_msg(
            "Avast! The scurvy gatekeeper says 'Could not generate a token with the username and password provided'. Check yer login details are shipshape!",
            severity=2)
        if "error" in token_json:
            output_msg(token_json["error"], severity=2)
        elif "message" in token_json:
            output_msg(token_json['message'], severity=2)
        raise ValueError("Token Error")


def get_all_the_layers(service_endpoint, tokenstring, session=None):
    """walk the endpoint and extract feature layer or map layer urls
    :param service_endpoint starting url
    :param tokenstring string containing token for authentication
    :param session: requests.Session
    """
    if session:
        response = session.get(service_endpoint + '?f=json' + tokenstring)
    else:
        response = requests.get(service_endpoint + '?f=json' + tokenstring)
    response.raise_for_status()
    service_layer_info = response.json()
    if service_layer_info.get('error'):
        raise Exception(f"Gaaar, 'service_call' failed to access {service_endpoint}: {service_layer_info.get('error')}")

    service_version = service_layer_info.get('currentVersion')

    service_layers_to_walk = []
    service_layers_to_get = []

    # search any folders
    if 'folders' in service_layer_info.keys() and len(service_layer_info.get('folders')) > 0:
        catalog_folder = service_layer_info.get('folders')
        folder_list = [f for f in catalog_folder if f.lower() not in 'utilities']
        for folder_name in folder_list:
            output_msg(f"Ahoy, I be searching {folder_name} for hidden treasure...", severity=0)
            lyr_list = get_all_the_layers(service_endpoint + '/' + folder_name, tokenstring, session=session)
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
        if session:
            response = session.get(url + '?f=json' + tokenstring)
            service_call = response.json()
        else:
            response = requests.get(url + '?f=json' + tokenstring)
            service_call = response.json()

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
                        lyr_list = get_all_the_layers(sub_layer_url, tokenstring, session=session)
                        if lyr_list:
                            service_layers_to_walk.extend(lyr_list)
                        # add the full url
                        else:
                            service_layers_to_get.append(sub_layer_url)
                    elif service_layer_type == 'sublayers':
                        # handled differently, drop the parent layer id and use sublayer id
                        sub_endpoint = url.rsplit('/', 1)[0]
                        sub_layer_url = sub_endpoint + '/' + lyr_id
                        lyr_list = get_all_the_layers(sub_layer_url, tokenstring, session=session)
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


def get_data(query, session=None):
    """ :param query: url query string
        :param session: requests.Session
        Download the data.
        Return a JSON object
    """
    try:
        if session:
            response = session.get(query)
        else:
            response = requests.get(query)
        response.raise_for_status()  # Raise for bad status codes
        resp_json = response.json()
        if resp_json.get('error'):
            output_msg(resp_json['error'])
        return resp_json
    except requests.RequestException as e:
        output_msg(str(e), severity=1)
        return {'error': str(e)}


def combine_data(fc_list, output_fc):
    """ :param fc_list: array of featureclass paths as strings
        :param output_fc: path to output dataset
        Combine the downloaded datafiles into one
        fastest approach is to use cursor
        Will drop spatial index on the destination for larger inputs to try and speed up insert
    """
    count_fc = len(fc_list)
    drop_spatial = False # whether to drop the spatial index before loading
    is_spatial = arcpy.Describe(fc_list[0]).dataType
    if count_fc > 50 and is_spatial == 'FeatureClass': # larger inputs
        drop_spatial = True

    if count_fc == 1:
        #simple case
        arcpy.Copy_management(fc_list[0], output_fc)
        output_msg(f"Created {output_fc}")
    else:

        for fc in fc_list:
            if fc_list.index(fc) == 0:
                # append to first dataset. much faster
                output_msg(f"Prepping yer first dataset {fc}")
                if arcpy.Exists(output_fc):
                    output_msg(f"Avast! {output_fc} exists, deleting...", severity=1)
                    arcpy.Delete_management(output_fc)
                
                arcpy.Copy_management(fc, output_fc)  # create dataset to append to
                output_msg(f"Created {output_fc}")
                if drop_spatial:
                    # delete the spatial index for better loading
                    output_msg("Dropping spatial index for loading performance")
                    arcpy.management.RemoveSpatialIndex(output_fc)

                fieldlist = []
                #fieldlist = ["SHAPE@"]
                fields = arcpy.ListFields(output_fc)
                for field in fields:
                    if field.name.lower() == u'shape':
                        fieldlist.insert(0, "SHAPE@") # add shape token to start
                    else:
                        fieldlist.append(field.name)
                
                insert_rows = arcpy.da.InsertCursor(output_fc, fieldlist)
            else:
                search_rows = arcpy.da.SearchCursor(fc, fieldlist) # append to first dataset
                for row in search_rows:
                    insert_rows.insertRow(row)
                del row, search_rows
                output_msg(f"Appended {fc}...")
        
        if drop_spatial:
            # recreate the spatial index
            output_msg("Adding spatial index")
            arcpy.management.AddSpatialIndex(output_fc)
        del insert_rows


def grouper(iterable, n, fillvalue=None):
    """ Cut iterable into n sized groups
        from itertools documentation, may not be most efficient, fillvalue causes issue
        :param iterable: object to iterate over
        :param n: int value to group
        :param fillvalue: value to fill with if chunk smaller than n
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def make_service_name(service_info, output_workspace, output_folder_path_len):
    global service_output_name_tracking_list
    global output_type

    # establish a unique name that isn't too long
    # 160 character limit for filegeodatabase
    max_path_length = 230  # sanity length for windows systems
    if output_type == 'Workspace':
        max_name_len = 150  # based on fgdb
    else:
        max_name_len = max_path_length - output_folder_path_len
    
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

    if output_folder_path_len  + service_name_len > max_path_length: # can be written to disc
        # shorten the service name
        max_len = max_path_length - output_folder_path_len
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
        ignore_ssl_verification = arcpy.GetParameter(10) # Bool - whether to ignore SSL verification (default True)
        ca_bundle_path = arcpy.GetParameterAsText(11) # String - path to CA bundle for SSL verification, if not ignoring

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
            query_str = urllib.parse.quote(query_str)

        if output_workspace == '':
            output_workspace = os.getcwd()

        if not os.path.exists(output_workspace):
            output_msg(f"Shiver me timbers, {output_workspace} doesn't exist! Trying to create it...")
            if output_workspace.endswith('.gdb'):
                arcpy.CreateFileGDB_management(os.path.dirname(output_workspace), os.path.basename(output_workspace))
            elif output_workspace.endswith('.sde'):
                output_msg("Aaar, can't create an SDE workspace for ya, that be beyond me powers. Create it yerself and point me to it!", severity=2)
            else:
                # assume folder
                os.makedirs(output_workspace)
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

        # build a generic session with the user agent spoofed and retries
        session = requests.Session()
        retry_strategy = Retry(
            total=max_tries,
            backoff_factor=sleep_time,
            status_forcelist=[429, 500, 502, 503, 504],  # Retry on these codes
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({'User-Agent': 'Mozilla/5.0'})

        if ignore_ssl_verification:
            warnings.simplefilter('ignore', InsecureRequestWarning)
            session.verify = False
        elif ca_bundle_path:
            session.verify = ca_bundle_path
        else:
            session.verify = True

        token = ''
        if username and not existing_token:
            token = get_token(username=username, password=password, referer=referring_domain, adapter_name=adapter_name,
                              client_type=token_client_type, session=session)
        elif existing_token:
            token = existing_token

        tokenstring = ''
        if len(token) > 0:
            tokenstring = '&token=' + token

        output_msg(f"Start the plunder! {service_endpoint}")
        output_msg(f"We be stashing the booty in {output_workspace}")

        service_layers_to_get = get_all_the_layers(service_endpoint, tokenstring, session=session)
        output_msg(f"Blimey, {len(service_layers_to_get)} layers for the pillagin'")
        for slyr in service_layers_to_get:
            count_tries = 0
            downloaded_fc_list = [] # for file merging.
            response = None
            current_iter = 0
            max_record_count = 0
            feature_count = 0
            final_fc = ''

            output_msg(f"Now pillagin' yer data from {slyr}")
            response = session.get(slyr + '?f=json' + tokenstring)
            service_info = response.json()

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
                                break
                else:
                    output_msg(f"No field list - come about using {objectid_field}!")

                # get count
                if query_str == '':
                    response = session.get(slyr + '/query?where=1%3D1&returnCountOnly=true&f=pjson' + tokenstring)
                else:
                    response = session.get(slyr + '/query?where=' + query_str + '&returnCountOnly=true&f=pjson' + tokenstring)
                feature_count = response.json()
                service_info[u'FeatureCount'] = feature_count.get('count')

                service_name_cl = make_service_name(service_info, output_workspace, len(output_folder))

                info_filename = service_name_cl + "_info.txt"
                info_file = os.path.join(output_folder, info_filename)

                # write out the service info for reference
                with open(info_file, 'w') as i_file:
                    json.dump(service_info, i_file, sort_keys=True, indent=4, separators=(',', ': '))
                    output_msg(f"Yar! {service_name_cl} Service info stashed in '{info_file}'")

                if supports_json:
                    try:
                        # to query using geometry,&geometry=   &geometryType= esriGeometryEnvelope &inSR= and probably spatial relationship and buffering
                        feat_data_query = r"/query?outFields=*&returnGeometry=true&returnIdsOnly=false&returnCountOnly=false&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&maxAllowableOffset=&geometryPrecision=&outSR=&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json" + tokenstring
                        if query_str =='':
                            feat_OIDLIST_query = r"/query?where=" + objectid_field + r"+%3E+0&returnGeometry=false&returnIdsOnly=true&returnCountOnly=false&returnExtentOnly=false&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=&maxAllowableOffset=&geometryPrecision=&outSR=&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json" + tokenstring
                        else:
                            feat_OIDLIST_query = r"/query?where=" + query_str + r"&returnGeometry=false&returnIdsOnly=true&returnCountOnly=false&returnExtentOnly=false&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Meter&outFields=&maxAllowableOffset=&geometryPrecision=&outSR=&orderByFields=&groupByFieldsForStatistics=&outStatistics=&resultOffset=&resultRecordCount=&returnZ=false&returnM=false&f=json" + tokenstring

                        max_record_count = service_info.get('maxRecordCount') # maximum number of records returned by service at once
                        if max_record_count > sanity_max_record_count:
                            output_msg(
                                "{0} max records is a wee bit large, using {1} instead...".format(max_record_count,
                                                                                                  sanity_max_record_count))
                            max_record_count = sanity_max_record_count

                        # extract using actual OID values is the safest way
                        feature_OIDs = None
                        response = session.get(slyr + feat_OIDLIST_query)
                        feature_OID_query = response.json()
                        if feature_OID_query and 'objectIds' in feature_OID_query:
                            feature_OIDs = feature_OID_query["objectIds"]
                        else:
                            output_msg(f"Blast, no OID values: {feature_OID_query}")

                        if feature_OIDs:
                            OID_count = len(feature_OIDs)
                            sortie_count = OID_count//max_record_count + (OID_count % max_record_count > 0)
                            output_msg(f"{OID_count} records, in chunks of {max_record_count}, err, that be {sortie_count} sorties. Ready lads!")

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
                                    where_clause = f"&where={objectid_field}+%3E%3D+{start_oid}+AND+{objectid_field}+%3C%3D+{end_oid}"
                                else:
                                    where_clause = f"&where={query_str}+AND+{objectid_field}+%3E%3D+{start_oid}+AND+{objectid_field}+%3C%3D+{end_oid}"
                                # response is a string of json with the attributes and geometry
                                query = slyr + feat_data_query + where_clause
                                response = get_data(query, session=session) # expects json object
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
                                        downloaded_fc_list.append(out_geofile)
                                        os.remove(out_JSON_file) # clean up the JSON file

                                    current_iter += 1
                        else:
                            raise ValueError("Aaar, plunderin' failed, feature OIDs is None")

                        # download complete, create a final output
                        if output_type == "Folder":
                            final_fc = os.path.join(output_workspace, service_name_cl + ".shp")
                        else:
                            final_fc = os.path.join(output_workspace, service_name_cl)

                        output_msg("Stashin' all the booty in '{0}'".format(final_fc))

                        #combine all the data
                        combine_data(fc_list=downloaded_fc_list, output_fc=final_fc)

                        #create_layer_file(service_info=service_info, service_name=service_name_cl, layer_source=final_fc, output_folder=output_folder)

                        elapsed_time = datetime.datetime.today() - start_time
                        output_msg("{0} plundered in {1}".format(final_fc, str(elapsed_time)))

                    except ValueError as e:
                        output_msg(str(e), severity=2)

                    except Exception as e:
                        line, err = trace()
                        output_msg("Script Error\n{0}\n on {1}".format(err, line), severity=2)
                        output_msg(arcpy.GetMessages())

                    finally:
                        if arcpy.Exists(final_fc):
                            data_count = int(arcpy.GetCount_management(final_fc)[0])
                            if data_count == OID_count: #we got it all
                                output_msg("Scrubbing the decks...")
                                for fc in downloaded_fc_list:
                                    arcpy.Delete_management(fc)
                            else:
                                output_msg("Splicin' the data failed - found {0} but expected {1}. Check {2} to see what went wrong.".format(data_count, OID_count, final_fc))
                else:
                    # no JSON output
                    output_msg("Aaaar, ye service does not support JSON output. Can't do it.")
            else:
                # service info error
                output_msg("Error: {0}".format(service_info.get('error')), severity=2)

    except ValueError as e:
        output_msg("ERROR: " + str(e), severity=2)

    except Exception as e:
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
