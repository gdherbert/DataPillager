# -*- coding: utf-8 -*-
# -------------------------------------------------------------------------------
# Purpose:     Extract data from an ArcGIS Service, in chunks defined by
#              the service Max Record Count to get around that limitation.
#              Requires that JSON is supported by the service

# Author:      Grant Herbert
#
# Created:     12/11/2014
# Copyright:   (c) Grant Herbert 2014
# Updated:    2026-06-19
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
    import itertools
    import re
    import warnings
    import shutil
except ImportError as e:
    print(e)
    sys.exit()

# --------
# globals
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
    """ Adds a Message if run as a tool
        or prints the message to the screen (standard output)
        :param msg: text to output
        :param severity: 0 = none, 1 = warning, 2 = error
    """
    try:
        in_tool = arcpy.GetParameterInfo() is not None
    except Exception:
        in_tool = False

    lines = str(msg).splitlines() or [str(msg)]

    for line in lines:
        if in_tool:
            if severity == 0:
                arcpy.AddMessage(line)
            elif severity == 1:
                arcpy.AddWarning(line)
            else:
                arcpy.AddError(line)
        else:
            print(line)


def create_session(ignore_ssl_verification=True, ca_bundle_path=None):
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
        # ignore overrides a CA bundle path
        warnings.simplefilter('ignore', InsecureRequestWarning)
        session.verify = False
    elif ca_bundle_path:
        session.verify = ca_bundle_path
    else:
        session.verify = True
    
    return session


def test_url(url_to_test, session=None):
    """test a url for validity (non-404)
    :param token_url: String
    """
    try:
        if session:
            response = session.get(url_to_test, timeout=10)
        else:
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
    assumes will be the same as service url. WIll not work with portal
    :param url_string: url of service
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
        if test_url(url2test, session=session):
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


def get_all_the_layers(service_endpoint, token, session=None):
    """walk the endpoint and extract feature layer or map layer urls
    :param: service_endpoint starting url
    :param: token for authentication
    :param: session: requests.Session
    :return: list of service layer urls to pillage
    """
    params = {'f': 'json'}
    if token:
        params["token"] = token
    
    service_layer_info = execute_query(service_endpoint, params=params, session=session)
    if service_layer_info.get('error'):
        raise Exception(f"Gaaar, 'service_call' failed to access {service_endpoint}: {service_layer_info.get('error')}")

    service_version = service_layer_info.get('currentVersion')

    service_layers_to_walk = []
    service_layers_to_get = []

    # search any folders
    if 'folders' in service_layer_info.keys() and len(service_layer_info.get('folders')) > 0:
        catalog_folder = service_layer_info.get('folders')
        folder_list = [f for f in catalog_folder if f.lower() != 'utilities']
        for folder_name in folder_list:
            output_msg(f"Ahoy, I be searching {folder_name} for hidden treasure...", severity=0)
            lyr_list = get_all_the_layers(service_endpoint + '/' + folder_name, token, session=session)
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
        service_call = execute_query(url, params=params, session=session)
        # for getting all the layers, start with a list of sublayers
        service_layers = None
        service_layer_type = None
        if service_call.get('layers'):
            service_layers = service_call.get('layers')
            service_layer_type = 'layers'
        elif service_call.get('subLayers'):
            service_layers = service_call.get('subLayers')
            service_layer_type = 'sublayers'

        # subLayers an array of objects, each has an id
        if service_layers is not None:
            # has sub layers, get em all
            for lyr in service_layers:
                if not lyr.get('subLayerIds'):  # ignore group layers
                    lyr_id = str(lyr.get('id'))
                    if service_layer_type == 'layers':
                        sub_layer_url = url + '/' + lyr_id
                        lyr_list = get_all_the_layers(sub_layer_url, token, session=session)
                        if lyr_list:
                            service_layers_to_walk.extend(lyr_list)
                        # add the full url
                        else:
                            service_layers_to_get.append(sub_layer_url)
                    elif service_layer_type == 'sublayers':
                        # handled differently, drop the parent layer id and use sublayer id
                        sub_endpoint = url.rsplit('/', 1)[0]
                        sub_layer_url = sub_endpoint + '/' + lyr_id
                        lyr_list = get_all_the_layers(sub_layer_url, token, session=session)
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


def execute_query(url, params=None, session=None):
    """ Download the data.
    :param url: url string
    :param: params: query parameters
    :param session: requests.Session
    :return: JSON object
    """
    try:
        if session:
            response = session.get(url, params=params, timeout=60)
        else:
            response = requests.get(url, params=params, timeout=60)

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
    try:
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
            for idx, fc in enumerate(fc_list):
                if idx == 0:
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
    except Exception as e:
        output_msg(f"Error combining data: {str(e)}", severity=2)


def grouper(iterable, n, fillvalue=None):
    """ Cut iterable into n sized groups
        from itertools documentation, may not be most efficient, fillvalue causes issue
        :param iterable: object to iterate over
        :param n: int value to group
        :param fillvalue: value to fill with if chunk smaller than n
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def get_attachments(layer_url, final_fc, oid_list, service_name, output_folder, output_workspace, clean_up_temp_attachments_data, session, token):
    """
    Download attachments from a feature service layer and add them
    to the destination feature class in a file geodatabase.
    
    layer_url : str         REST URL to the feature layer
    final_fc : str          Path to the output feature class in the target geodatabase
    oid_list : list[int]    ObjectIDs to request attachments for
    service_name : str      Name used for output folders/files
    output_folder : str     Folder for downloaded attachment files / metadata
    output_workspace : str  Output GDB path
    session : requests.Session          Existing session
    token : str        Token string
    """

    def _safe_filename(name):
        """Remove filesystem-invalid characters."""
        return re.sub(r'[<>:"/\\\\|?*]', "_", name)

    try:
        att_folders = []
        # Check whether the service layer has attachments enabled, and has attachments to download
        layer_params = {"f": "json"}
        if token:
            layer_params["token"] = token

        layer_info = execute_query(layer_url, params=layer_params, session=session)

        if not layer_info.get("hasAttachments", False):
            output_msg(f"{layer_url} has no attachments enabled. {final_fc} attachments skipped.")
            return
        else:
            # we enable attachments because it exists on source, even if empty
            output_msg(f"{layer_url} has attachments enabled, replicating on {final_fc}...")
            try:
                arcpy.management.EnableAttachments(final_fc)
                output_msg(f"Enabled attachments on target {final_fc}.")
            except Exception as e:
                output_msg(f"Error enabling attachments on {final_fc}: {str(e)}", severity=1)
        
        # =====================================================================
        # Check if ANY records have attachments, if not then no download needed
        # stop on first attachment found
        # Use pagination with resultRecordCount to improve performance
        output_msg("Checking for attachments across all records...")
        
        has_any_attachments = False
        batch_size = 50
        
        for i in range(0, len(oid_list), batch_size):
            batch = oid_list[i:i + batch_size]
            oid_str = ",".join(map(str, batch))
            
            query_url = f"{layer_url}/queryAttachments"
            query_params = {
                "objectIds": oid_str,
                "f": "json"
            }
            if token:
                query_params["token"] = token
            
            try:
                att_data = execute_query(query_url, params=query_params, session=session)
                
                # Check if any attachmentGroups have attachmentInfos
                for att_group in att_data.get("attachmentGroups", []):
                    if att_group.get("attachmentInfos"):
                        has_any_attachments = True
                        break
                
                if has_any_attachments:
                    output_msg("Attachments found, proceeding with download...")
                    break  # Found attachments, stop checking batches
                    
            except Exception as e:
                output_msg(f"Warning: Error checking for attachments: {str(e)}", severity=1)
                return
        
        if not has_any_attachments:
            output_msg("No attachments found on any records, skipping download.")
            return
        
        # there are attachments, create download folder, and match table for loading
        attachments_folder = os.path.join(output_folder, f"{service_name}_attachments")
        attachments_json = os.path.join(output_folder, f"{service_name}_attachments.json")
        os.makedirs(attachments_folder, exist_ok=True)
        att_folders.append(attachments_folder) # tracking for cleanup later

        # ---------------------------------------------------------------------
        # Download attachment metadata + files
        # ---------------------------------------------------------------------
        all_attachments = {}
        attachment_count = 0

        output_msg("Downloading attachments...")

        batch_size = 50
        for i in range(0, len(oid_list), batch_size):
            batch = oid_list[i:i + batch_size]
            oid_str = ",".join(map(str, batch))

            query_url = f"{layer_url}/queryAttachments"
            query_params = {
                "objectIds": oid_str,
                "f": "json",
                "resultRecordCount": 10000  # Ensure we get all attachments for this batch
            }
            if token:
                query_params["token"] = token

            att_data = execute_query(query_url, params=query_params, session=session)

            for att_group in att_data.get("attachmentGroups", []):
                parent_oid = att_group.get("parentObjectId")
                infos = att_group.get("attachmentInfos", [])

                if not infos:
                    continue

                all_attachments.setdefault(parent_oid, [])

                for att_info in infos:
                    att_id = att_info.get("id")
                    att_name = _safe_filename(att_info.get("name", f"attachment_{att_id}"))

                    att_download_url = f"{layer_url}/{parent_oid}/attachments/{att_id}"
                    download_params = {}
                    if token:
                        download_params["token"] = token

                    try:
                        att_response = session.get(att_download_url, params=download_params)
                        att_response.raise_for_status()
                        #a subfolder per OID in case of duplicate names, keep original name
                        att_subfolder = os.path.join(attachments_folder, str(parent_oid))
                        os.makedirs(att_subfolder, exist_ok=True)
                        att_file_path = os.path.join(att_subfolder, f"{att_name}")

                        with open(att_file_path, "wb") as f:
                            f.write(att_response.content)

                        att_info["local_path"] = att_file_path
                        all_attachments[parent_oid].append(att_info)
                        attachment_count += 1

                    except Exception as e:
                        output_msg(
                            f"Warning: Could not download attachment {att_name}: {str(e)}",
                            severity=1
                        )

        if not all_attachments:
            output_msg("No attachment files were returned.")
            return

        with open(attachments_json, "w", encoding="utf-8") as f:
            json.dump(all_attachments, f, indent=2, default=str)

        output_msg(f"Downloaded {attachment_count} attachment(s)")
        output_msg(f"Attachment metadata saved to {attachments_json}")
        output_msg(f"Attachment files stored in {attachments_folder}")

        # ---------------------------------------------------------------------
        # Enable attachments on target FC if needed
        # ---------------------------------------------------------------------
        if not (output_workspace.lower().endswith(".gdb") or output_workspace.lower().endswith(".sde")):
            output_msg("Output workspace does not accept attachments, skipping.")
            return

        desc = arcpy.Describe(final_fc)
        oid_field = desc.OIDFieldName

        # ---------------------------------------------------------------------
        # Create a temporary MATCH TABLE with join field + file path
        # ---------------------------------------------------------------------
        table_name = f"{service_name}_ATTMATCH"
        match_table = os.path.join(output_workspace, table_name)
        
        if arcpy.Exists(match_table):
            arcpy.management.Delete(match_table)

        arcpy.management.CreateTable(output_workspace, table_name)
        arcpy.management.AddField(match_table, "MATCHID", "LONG")
        arcpy.management.AddField(match_table, "FILENAME", "TEXT", field_length=1000)

        with arcpy.da.InsertCursor(match_table, ["MATCHID", "FILENAME"]) as icur:
            for parent_oid, attachments in all_attachments.items():
                for att in attachments:
                    local_path = att.get("local_path")
                    if local_path and os.path.exists(local_path):
                        icur.insertRow([parent_oid, local_path])

        # ---------------------------------------------------------------------
        # 5) Add attachments using the match table
        # ---------------------------------------------------------------------
        arcpy.management.AddAttachments(
            in_dataset=final_fc,
            in_join_field=oid_field,
            in_match_table=match_table,
            in_match_join_field="MATCHID",
            in_match_path_field="FILENAME"
        )

        output_msg("Attachments successfully added to target feature class.")

        # Clean up temporary data
        if clean_up_temp_attachments_data:
            output_msg("Cleaning up temporary attachment files and match table...")
            # delete all the folders. 
            for folder in att_folders:
                if os.path.exists(folder):
                    try:
                        shutil.rmtree(folder)
                        output_msg(f"Deleted attachment folder {folder}")
                    except Exception as e:
                        output_msg(f"Warning: Could not delete {folder}: {str(e)}", severity=1)
            
            try:
                arcpy.management.Delete(match_table)
                output_msg("Cleaned up temporary attachment match table.")
            except Exception as e:
                output_msg(f"Warning: Could not delete temporary match table: {str(e)}", severity=1)

    except Exception as e:
        output_msg(f"Warning: Could not download/add attachments: {str(e)}", severity=1)


def make_service_name(service_info, output_workspace):
    global service_output_name_tracking_list
    global output_type

    # establish a unique name that isn't too long
    max_path_length = 259  # sanity length for windows systems (max 260 char)
    if output_type == "Folder":
        # has to be able to handle being a .shp.xml
        max_path_length = 250  # sanity length for windows systems (max 260 char)
    
    workspace_len = len(output_workspace)
    max_name_len = max_path_length - workspace_len
    
    parent_name = ''
    parent_id = ''
    service_name = service_info.get('name')
    service_id = str(service_info.get('id'))

    # clean up the service name (remove invalid characters, multiple underscores)
    service_name_cl = service_name.encode('ascii', 'ignore').decode('ascii')  # strip any non-ascii characters that may cause an issue
    service_name_cl = arcpy.ValidateTableName(service_name_cl, output_workspace)
    service_name_cl = re.sub(r'_+', '_', service_name_cl).rstrip('_')

    if len(service_name_cl) > max_name_len:
        service_name_cl = service_name_cl[:max_name_len]

    service_name_len = len(service_name_cl)

    if service_info.get('parentLayer'):
        parent_name = service_info.get('parentLayer').get('name')
        parent_id = str(service_info.get('parentLayer').get('id'))

    if workspace_len + service_name_len > max_path_length: # can't be written to disc
        # shorten the service name
        max_len = max_path_length - workspace_len
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

def pillage_the_layer(slyr, token, output_folder, output_workspace, session, overwrite_output, strict_mode,
                      query_str, create_empty_schema, include_attachments, clean_up_temp_attachments_data):
    """
    pillage the data from the service url
    :param slyr: str: service url to pillage
    :param token: str: token for authentication
    :param output_folder: str: folder to save intermediate files and final outputs
    :param output_workspace: str: geodatabase or folder to save final feature class
    :param session: requests session for making API calls
    :param overwrite_output: boolean: whether to overwrite existing output
    :param strict_mode: boolean: whether to enforce strict mode (requires JSON support)
    :param query_str: str: optional SQL query string for filtering features
    :param create_empty_schema: boolean: whether to create empty featureclass if no data found
    :param include_attachments: boolean: whether to download attachments
    :param clean_up_temp_attachments_data: boolean: whether to clean up temporary attachment files after loading
    :returns: str: Success or Failure (error)
    """
    global max_tries
    global sleep_time
    global service_output_name_tracking_list
    global output_type
    global sanity_max_record_count

    try:    
        count_tries = 0
        downloaded_fc_list = [] # for file merging.
        response = None
        current_iter = 0
        max_record_count = 0
        feature_count = 0
        final_fc = ''
        OID_count = 0
        slyr_start_time = datetime.datetime.today()

        output_msg(f"Now pillagin' yer data from {slyr}")
        
        json_param = {"f": "json"}
        if token:
            json_param["token"] = token
        service_info = execute_query(slyr, params=json_param, session=session)

        if not service_info.get('error'):
            # add url to info
            service_info[u'serviceURL'] = slyr

            # assume JSON supported
            supports_json = True
            if strict_mode:
                # check JSON supported
                supports_json = False
                if 'supportedQueryFormats' in service_info:
                    supported_formats = [f.strip() for f in service_info.get('supportedQueryFormats').split(",")]
                    for data_format in supported_formats:
                        if data_format == "JSON":
                            supports_json = True
                            break
                else:
                    output_msg("Strict mode scuttled, no supported formats, forgin' on", severity=1)

            objectid_field = "OBJECTID"
            field_list = None
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
                where_clause = f"1=1"
            else:
                where_clause = query_str

            ct_params = {
                "where": where_clause,
                "returnCountOnly": "true",
                "f": "json"
            }
            if token:
                ct_params["token"] = token

            feature_count = execute_query(f'{slyr}/query', params=ct_params, session=session)
            
            service_info[u'FeatureCount'] = feature_count.get('count')

            service_name_cl = make_service_name(service_info, output_workspace)

            # make the final_featureclass name and check if it exists
            if output_type == "Folder":
                final_fc = os.path.join(output_workspace, service_name_cl + ".shp")
            else:
                final_fc = os.path.join(output_workspace, service_name_cl)

            if arcpy.Exists(final_fc) and not overwrite_output:
                output_msg(f"Avast! {final_fc} exists and overwrite_output is set to False. Skippin' it...", severity=1)
                # skip it, don't try to plunder or combine data
                raise ValueError(f"{final_fc} exists, skipped")
            
            info_filename = service_name_cl + "_info.txt"
            info_file = os.path.join(output_folder, info_filename)

            # write out the service info for reference
            with open(info_file, 'w') as i_file:
                json.dump(service_info, i_file, sort_keys=True, indent=4, separators=(',', ': '))
                output_msg(f"Yar! {service_name_cl} Service info stashed in '{info_file}'")

            if supports_json:
                try:
                    # get the OIDs
                    if query_str == '':
                        where_clause = f"{objectid_field} > 0"
                    else:
                        where_clause = query_str

                    oid_params = {
                        "where": where_clause,
                        "returnGeometry": "false",
                        "returnIdsOnly": "true",
                        "returnCountOnly": "false",
                        "returnExtentOnly": "false",
                        "f": "json"
                    }
                    if token:
                        oid_params["token"] = token

                    max_record_count = service_info.get('maxRecordCount') # maximum number of records returned by service at once
                    if max_record_count > sanity_max_record_count:
                        output_msg(
                            "{0} max records is a wee bit large, using {1} instead...".format(max_record_count,
                                                                                                sanity_max_record_count))
                        max_record_count = sanity_max_record_count

                    # extract using actual OID values is the safest way
                    feature_OIDs = None

                    feature_OID_query = execute_query(f"{slyr}/query", params=oid_params, session=session)
                    
                    if feature_OID_query and 'objectIds' in feature_OID_query:
                        feature_OIDs = feature_OID_query["objectIds"]
                    else:
                        output_msg(f"Blast, no OID values: {feature_OID_query}")

                    if feature_OIDs:
                        feat_data_params_base = {
                                "outFields": "*",
                                "returnGeometry": "true",
                                "returnIdsOnly": "false",
                                "returnCountOnly": "false",
                                "returnExtentOnly": "false",
                                "spatialRel": "esriSpatialRelIntersects",
                                "units": "esriSRUnit_Meter",
                                "returnZ": "false",
                                "returnM": "false",
                                "f": "json"
                            }
                        
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

                            # query in batches
                            if query_str == '':
                                where_clause = f"{objectid_field} >= {start_oid} AND {objectid_field} <= {end_oid}"
                            else:
                                where_clause = f"{query_str} AND {objectid_field} >= {start_oid} AND {objectid_field} <= {end_oid}"

                            params = feat_data_params_base.copy()
                            params["where"] = where_clause

                            if token:
                                params["token"] = token

                            response = execute_query(f"{slyr}/query", params=params, session=session)

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
                                    
                                    downloaded_fc_list.append(out_geofile)
                                    os.remove(out_JSON_file) # clean up the JSON file

                                current_iter += 1
                    else:
                        # no OIDs, empty
                        if create_empty_schema:
                            # create an empty featureclass with the correct schema
                            final_fc_name = os.path.basename(final_fc)
                            output_msg(f"No OID values found, creating an empty {final_fc_name} with schema")

                            # Determine geometry type
                            esri_to_arcpy_geom = {
                                'esriGeometryPoint': 'POINT',
                                'esriGeometryPolyline': 'POLYLINE',
                                'esriGeometryPolygon': 'POLYGON'
                            }
                            geometry_type = esri_to_arcpy_geom.get(
                                service_info.get('geometryType'), 
                                'POINT'
                            )
                            
                            # Get spatial reference if available
                            spatial_ref = None
                            if 'extent' in service_info and 'spatialReference' in service_info['extent']:
                                sr_info = service_info['extent']['spatialReference']
                                if 'wkid' in sr_info:
                                    spatial_ref = arcpy.SpatialReference(sr_info['wkid'])
                            
                            # Create empty featureclass
                            arcpy.CreateFeatureclass_management(output_workspace, final_fc_name, geometry_type, spatial_reference=spatial_ref)
                            
                            # Add fields from service schema
                            if field_list:
                                for field in field_list:
                                    field_name = field.get('name')
                                    field_type = field.get('type')
                                    
                                    # Skip OID and SHAPE fields
                                    if field_type in ['esriFieldTypeOID', 'esriFieldTypeGeometry']:
                                        continue
                                    
                                    # Map esri field types to arcpy field types
                                    arcpy_type = 'TEXT'
                                    if field_type == 'esriFieldTypeInteger':
                                        arcpy_type = 'LONG'
                                    elif field_type == 'esriFieldTypeSmallInteger':
                                        arcpy_type = 'SHORT'
                                    elif field_type == 'esriFieldTypeSingle':
                                        arcpy_type = 'FLOAT'
                                    elif field_type == 'esriFieldTypeDouble':
                                        arcpy_type = 'DOUBLE'
                                    elif field_type == 'esriFieldTypeString':
                                        arcpy_type = 'TEXT'
                                    elif field_type == 'esriFieldTypeDate':
                                        arcpy_type = 'DATE'
                                    
                                    field_length = field.get('length', 255) if arcpy_type == 'TEXT' else None
                                    try:
                                        arcpy.AddField_management(final_fc, field_name, arcpy_type, field_length=field_length)
                                    except:
                                        output_msg(f"Failed to add field: {field_name}", severity=2)
                            
                            output_msg(f"Created empty featureclass: {final_fc}")

                        else:
                            raise ValueError("Aaar, plunderin' failed, feature OIDs is None")

                    if len(downloaded_fc_list) > 0:
                        # download complete, create a final output
                        output_msg("Stashin' all the booty in '{0}'".format(final_fc))

                        #combine all the data
                        combine_data(fc_list=downloaded_fc_list, output_fc=final_fc)

                    if arcpy.Exists(final_fc):
                        data_count = int(arcpy.GetCount_management(final_fc)[0])
                        if data_count == OID_count: #we got it all
                            output_msg("Scrubbing the decks...")
                            scrub_the_decks(downloaded_fc_list)
                        else:
                            # count issue, jump out
                            msg = f"Splicin' the data failed - found {data_count} but expected {OID_count}. Check {final_fc} to see what went wrong."
                            raise ValueError(msg) 
                    
                    # download attachments if requested
                    if include_attachments and feature_OIDs:
                        get_attachments(slyr, final_fc, feature_OIDs, service_name_cl, output_folder, output_workspace, clean_up_temp_attachments_data, session, token)

                    msg = f"{slyr} plundered to {final_fc} in {datetime.datetime.today() - slyr_start_time}"
                    output_msg(msg)
                    return f"Success: {msg}"

                except ValueError as e:
                    output_msg(str(e), severity=2)
                    return f"Error: {e}"

                except Exception as e:
                    line, err = trace()
                    output_msg(f"Script Error\n{err}\n on {line}", severity=2)
                    output_msg(arcpy.GetMessages())
                    return f"Error {err} on {line}: {e}"

            else:
                # no JSON output
                msg = "Aaaar, ye service does not support JSON output. Can't do it."
                output_msg(msg)
                return f"Failed: {msg}"
        else:
            # service info error
            msg = f"{service_info.get('error')}"
            output_msg(msg, severity=2)
            return f"Error: {msg}"

    except Exception as e:
        line, err = trace()
        msg = f"Script Error\n{err}\n on {line}"
        output_msg(msg, severity=2)
        output_msg(arcpy.GetMessages())
        return msg


def scrub_the_decks(fc_list):
    """delete the temporary featureclasses created during the process
    :param fc_list: list of featureclass paths to delete
    """
    for fc in fc_list:
        try:
            arcpy.Delete_management(fc)
            output_msg(f"Deleted {fc}")
        except Exception as e:
            output_msg(f"Warning: Could not delete {fc}: {str(e)}", severity=1)
            continue

#-------------------------------------------------
def main():
    global count_tries
    global max_tries
    global sleep_time
    global service_output_name_tracking_list
    global output_type
    global sanity_max_record_count
    
    start_time = datetime.datetime.today()
    session = None
    overwrite_output = None
    user_overwrite_setting = arcpy.env.overwriteOutput

    try:
        # arcgis toolbox parameters
        service_endpoint = arcpy.GetParameterAsText(0) # String - URL of Service endpoint required
        output_workspace = arcpy.GetParameterAsText(1) # String - gdb/folder to put the results required
        max_tries = arcpy.GetParameter(2) # Int - max number of retries allowed required
        sleep_time = arcpy.GetParameter(3) # Int - max number of retries allowed required
        strict_mode = arcpy.GetParameter(4) # Bool - JSON check True/False required
        username = arcpy.GetParameterAsText(5) # String - username optional
        password = arcpy.GetParameterAsText(6) # String - password optional
        referring_domain = arcpy.GetParameterAsText(7) # String - url of auth domain
        existing_token = arcpy.GetParameterAsText(8) # String - valid token value
        query_str = arcpy.GetParameterAsText(9) # String - valid SQL query string
        ignore_ssl_verification = arcpy.GetParameter(10) # Bool - whether to ignore SSL verification (default True)
        ca_bundle_path = arcpy.GetParameterAsText(11) # String - path to CA bundle for SSL verification, if not ignoring
        create_empty_schema = arcpy.GetParameter(12) # Bool - whether to create an empty featureclass if no data found (default False)
        overwrite_output = arcpy.GetParameter(13) # Bool - whether to overwrite existing output (default True)
        include_attachments = arcpy.GetParameter(14) # Bool - whether to download attachments (default False)
        clean_up_temp_attachments_data = arcpy.GetParameter(15) # Bool - whether to clean up downloaded attachment files after adding to FC, match table, etc (default False)
        
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
                if not os.path.exists(os.path.dirname(output_workspace)):
                    os.makedirs(os.path.dirname(output_workspace))
                arcpy.CreateFileGDB_management(os.path.dirname(output_workspace), os.path.basename(output_workspace))
            elif output_workspace.endswith('.sde'):
                msg = "Aaar, can't create an SDE workspace for ya, that be beyond me powers. Create it yerself and point me to it!"
                output_msg(msg, severity=2)
                raise ValueError(msg)
            else:
                # assume folder
                os.makedirs(output_workspace)

        output_desc = arcpy.Describe(output_workspace)
        output_type = output_desc.dataType

        if output_type == "Folder": # To Folder
            output_folder = output_workspace
        else:
            output_folder = output_desc.path

        arcpy.env.overwriteOutput = overwrite_output

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
        session = create_session(ignore_ssl_verification, ca_bundle_path)
        
        token = ''
        if username and not existing_token:
            token = get_token(username=username, password=password, referer=referring_domain, adapter_name=adapter_name,
                              client_type=token_client_type, session=session)
        elif existing_token:
            token = existing_token

        if include_attachments:
            output_msg("Arrr, ye be wantin' to plunder attachments too! That be a mighty fine choice, \
            but beware it may take longer and eat up more of yer storage space!")
        
        # start the work
        output_msg(f"Start the plunder! {service_endpoint}")
        output_msg(f"We be stashing the booty in {output_workspace}")

        service_layers_to_get = get_all_the_layers(service_endpoint, token, session=session)
        output_msg(f"Blimey, {len(service_layers_to_get)} layers for the pillagin'")
        
        slyr_tracker = {}
        for slyr in service_layers_to_get:
            slyr_tracker[slyr] = pillage_the_layer(slyr, token, output_folder, output_workspace, session, overwrite_output, strict_mode,
                                                   query_str, create_empty_schema, include_attachments, clean_up_temp_attachments_data)

        for slyr, result in slyr_tracker.items():
            output_msg(f"{slyr} plunder result: {result}")

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
        if user_overwrite_setting is not None: # revert to original setting
            arcpy.env.overwriteOutput = user_overwrite_setting
        if session is not None:
            session.close()
        output_msg(f"Plunderin' done, in {datetime.datetime.today() - start_time}")


if __name__ == '__main__':
    main()
