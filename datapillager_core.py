# -*- coding: utf-8 -*-
"""Core business logic for DataPillager.

This module is intentionally toolbox-agnostic: callers pass configuration and
an optional message callback.
"""

import codecs
import datetime
import itertools
import json
import os
import re
import shutil
import traceback
import urllib.parse
import warnings

import arcpy
import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
from urllib3.util.retry import Retry


class DataPillagerError(Exception):
    """Raised for expected operational failures in the pillaging workflow."""


CORE_VERSION = "v2.4.0"


class DataPillagerRunner:
    @staticmethod
    def _to_bool(value, default=False):
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in ("true", "t", "1", "yes", "y", "on"):
                return True
            if lowered in ("false", "f", "0", "no", "n", "off", ""):
                return False
        return bool(value)

    def __init__(self, config, message_handler=None):
        self.config = config
        self.message_handler = message_handler

        self.max_tries = int(config.get("max_tries", 5))
        self.sleep_time = int(config.get("sleep_time", 2))
        self.strict_mode = self._to_bool(config.get("strict_mode"), default=True)

        self.service_endpoint = (config.get("service_endpoint") or "").strip()
        self.output_workspace = (config.get("output_workspace") or "").strip()
        self.username = (config.get("username") or "").strip()
        self.password = config.get("password") or ""
        self.referring_domain = (config.get("referring_domain") or "").strip()
        self.existing_token = (config.get("existing_token") or "").strip()
        self.query_str = (config.get("query_str") or "").strip()

        self.enforce_ssl_verification = self._to_bool(config.get("enforce_ssl_verification"), default=False)
        self.ca_bundle_path = (config.get("ca_bundle_path") or "").strip()

        self.create_empty_schema = self._to_bool(config.get("create_empty_schema"), default=False)
        self.overwrite_output = self._to_bool(config.get("overwrite_output"), default=True)
        self.preserve_global_ids = self._to_bool(config.get("preserve_global_ids"), default=True)
        self.write_service_info = self._to_bool(config.get("write_service_info"), default=True)
        self.include_attachments = self._to_bool(config.get("include_attachments"), default=False)
        self.clean_up_temp_attachments_data = self._to_bool(config.get("clean_up_temp_attachments_data"), default=False)

        self.sanity_max_record_count = 10000
        self.service_output_name_tracking_list = []
        self.output_type = None

        self.session = None
        self.user_overwrite_setting = arcpy.env.overwriteOutput
        self.user_preserve_globalids_setting = getattr(arcpy.env, "preserveGlobalIds", None)

    def _emit(self, msg, severity=0):
        lines = str(msg).splitlines() or [str(msg)]
        for line in lines:
            if self.message_handler:
                self.message_handler(line, severity)
            else:
                print(line)

    @staticmethod
    def trace():
        tb = traceback.format_exc()
        last_line = tb.splitlines()[-1] if tb else "Unknown error"
        return last_line

    def create_session(self):
        session = requests.Session()
        retry_strategy = Retry(
            total=self.max_tries,
            backoff_factor=self.sleep_time,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        if not self.enforce_ssl_verification:
            warnings.simplefilter("ignore", InsecureRequestWarning)
            session.verify = False
        elif self.ca_bundle_path:
            session.verify = self.ca_bundle_path
        else:
            session.verify = True

        return session

    def test_url(self, url_to_test):
        try:
            response = self.session.get(url_to_test, timeout=10)
            if response.status_code == 200:
                self._emit(f"Ho, a successful url test: {url_to_test}")
                return url_to_test
        except requests.RequestException:
            pass
        return None

    @staticmethod
    def get_adapter_name(url_string):
        parsed = urllib.parse.urlparse(url_string)
        if "arcgis.com" in parsed.netloc:
            return parsed.path.split("/")[2]
        return parsed.path.split("/")[1]

    @staticmethod
    def get_referring_domain(url_string):
        parsed = urllib.parse.urlparse(url_string)
        if "arcgis.com" in parsed.netloc:
            return "https://www.arcgis.com"
        if parsed.scheme == "http":
            return urllib.parse.urlunsplit(["https", parsed.netloc, "", "", ""])
        return urllib.parse.urlunsplit([parsed.scheme, parsed.netloc, "", "", ""])

    def get_token(self, referer, adapter_name, client_type="requestip", expiration=240):
        query_dict = {
            "username": self.username,
            "password": self.password,
            "expiration": str(expiration),
            "client": client_type,
            "referer": referer,
            "f": "json",
        }

        token_url = None
        token_url_array = [
            f"{referer}/sharing/rest/generateToken",
            f"{referer}/{adapter_name}/tokens/generateToken",
        ]

        for url_to_test in token_url_array:
            if self.test_url(url_to_test):
                token_url = url_to_test
                break

        if not token_url:
            raise DataPillagerError("Unable to locate token endpoint for the provided service")

        response = self.session.post(token_url, data=query_dict)
        token_json = response.json()

        if "token" in token_json:
            return token_json["token"]

        if "error" in token_json:
            self._emit(token_json["error"], severity=2)
        elif "message" in token_json:
            self._emit(token_json["message"], severity=2)

        raise DataPillagerError("Could not generate a token with the username and password provided")

    def execute_query(self, url, params=None):
        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            resp_json = response.json()
            if resp_json.get("error"):
                self._emit(resp_json["error"], severity=1)
            return resp_json
        except requests.RequestException as ex:
            self._emit(str(ex), severity=1)
            return {"error": str(ex)}

    def get_all_the_layers(self, service_endpoint, token):
        params = {"f": "json"}
        if token:
            params["token"] = token

        service_layer_info = self.execute_query(service_endpoint, params=params)
        if service_layer_info.get("error"):
            raise DataPillagerError(
                f"Gaaar, service_call failed to access {service_endpoint}: {service_layer_info.get('error')}"
            )

        service_layers_to_walk = []
        service_layers_to_get = []

        if service_layer_info.get("folders"):
            folder_list = [f for f in service_layer_info["folders"] if f.lower() != "utilities"]
            for folder_name in folder_list:
                self._emit(f"Ahoy, I be searching {folder_name} for hidden treasure...")
                lyr_list = self.get_all_the_layers(f"{service_endpoint}/{folder_name}", token)
                if lyr_list:
                    service_layers_to_walk.extend(lyr_list)

        if service_layer_info.get("services"):
            for service in service_layer_info["services"]:
                service_type = service["type"]
                service_name = service["name"]
                if service_type in ["MapServer", "FeatureServer"]:
                    service_url = f"{service_endpoint}/{service_name}/{service_type}"
                    if "/" in service_name:
                        folder, sname = service_name.split("/")
                        if service_endpoint.endswith(folder):
                            service_url = f"{service_endpoint}/{sname}/{service_type}"
                    service_layers_to_walk.append(service_url)

        if not service_layers_to_walk:
            service_layers_to_walk.append(service_endpoint)

        for url in service_layers_to_walk:
            service_call = self.execute_query(url, params=params)
            service_layers = service_call.get("layers") or service_call.get("subLayers")
            service_layer_type = "layers" if service_call.get("layers") else "sublayers"

            if service_layers is not None:
                for lyr in service_layers:
                    if not lyr.get("subLayerIds"):
                        lyr_id = str(lyr.get("id"))
                        if service_layer_type == "layers":
                            sub_layer_url = f"{url}/{lyr_id}"
                        else:
                            sub_endpoint = url.rsplit("/", 1)[0]
                            sub_layer_url = f"{sub_endpoint}/{lyr_id}"

                        lyr_list = self.get_all_the_layers(sub_layer_url, token)
                        if lyr_list:
                            service_layers_to_walk.extend(lyr_list)
                        else:
                            service_layers_to_get.append(sub_layer_url)
            elif service_call.get("type") not in ("Group Layer", "Raster Layer"):
                service_layers_to_get.append(url)

        return service_layers_to_get

    def combine_data(self, fc_list, output_fc):
        try:
            count_fc = len(fc_list)
            drop_spatial = False
            is_spatial = arcpy.Describe(fc_list[0]).dataType
            if count_fc > 50 and is_spatial == "FeatureClass":
                drop_spatial = True

            if count_fc == 1:
                arcpy.Copy_management(fc_list[0], output_fc)
                self._emit(f"Created {output_fc}")
                return

            fieldlist = None
            insert_rows = None

            for idx, fc in enumerate(fc_list):
                if idx == 0:
                    if arcpy.Exists(output_fc):
                        self._emit(f"Avast! {output_fc} exists, deleting...", severity=1)
                        arcpy.Delete_management(output_fc)

                    arcpy.Copy_management(fc, output_fc)
                    self._emit(f"Created {output_fc}")

                    if drop_spatial:
                        self._emit("Dropping spatial index for loading performance")
                        arcpy.management.RemoveSpatialIndex(output_fc)

                    fieldlist = []
                    for field in arcpy.ListFields(output_fc):
                        if field.name.lower() == "shape":
                            fieldlist.insert(0, "SHAPE@")
                        else:
                            fieldlist.append(field.name)

                    insert_rows = arcpy.da.InsertCursor(output_fc, fieldlist)
                else:
                    search_rows = arcpy.da.SearchCursor(fc, fieldlist)
                    for row in search_rows:
                        insert_rows.insertRow(row)
                    del search_rows
                    self._emit(f"Appended {fc}...")

            if drop_spatial:
                self._emit("Adding spatial index")
                arcpy.management.AddSpatialIndex(output_fc)

            if insert_rows:
                del insert_rows
        except Exception as ex:
            self._emit(f"Error combining data: {ex}", severity=2)
            raise

    @staticmethod
    def grouper(iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return itertools.zip_longest(*args, fillvalue=fillvalue)

    @staticmethod
    def chunk_list(values, chunk_size):
        for idx in range(0, len(values), chunk_size):
            yield values[idx : idx + chunk_size]

    def get_attachments(self, layer_url, final_fc, oid_list, service_name, output_folder, output_workspace, token):
        def _safe_filename(name):
            return re.sub(r"[<>:\"/\\|?*]", "_", name)

        try:
            att_folders = []
            query_url = f"{layer_url}/queryAttachments"
            attachment_groups = []
            batch_size = 250
            batch_count = (len(oid_list) + batch_size - 1) // batch_size
            self._emit(
                f"Attachment query batching enabled: {len(oid_list)} OIDs across {batch_count} batches of up to {batch_size}"
            )

            # Avoid 414 URI Too Long by POSTing objectIds in manageable batches.
            for batch_index, oid_batch in enumerate(self.chunk_list(oid_list, batch_size), start=1):
                query_params = {
                    "objectIds": ",".join(str(oid) for oid in oid_batch),
                    "f": "json",
                }
                if token:
                    query_params["token"] = token

                try:
                    self._emit(f"queryAttachments batch {batch_index}/{batch_count}: POST ({len(oid_batch)} OIDs)")
                    response = self.session.post(query_url, data=query_params, timeout=120)
                    response.raise_for_status()
                except requests.RequestException:
                    self._emit(f"queryAttachments batch {batch_index}/{batch_count}: POST failed, trying GET fallback", severity=1)
                    response = self.session.get(query_url, params=query_params, timeout=120)
                    response.raise_for_status()
                att_data = response.json()
                if att_data.get("error"):
                    raise DataPillagerError(f"queryAttachments failed: {att_data.get('error')}")

                attachment_groups.extend(att_data.get("attachmentGroups", []))

            if not attachment_groups:
                self._emit("No attachments found for this layer")
                return

            if not arcpy.Describe(final_fc).path.lower().endswith(".gdb"):
                raise DataPillagerError("Attachments require file geodatabase output")

            try:
                arcpy.management.EnableAttachments(final_fc)
            except Exception:
                # already enabled or unsupported edge case; AddAttachments will fail if truly invalid
                pass

            table_name = arcpy.ValidateTableName(f"{service_name}_attachment_match", output_workspace)
            temp_match_table = os.path.join(output_workspace, table_name)
            if arcpy.Exists(temp_match_table):
                arcpy.management.Delete(temp_match_table)

            arcpy.management.CreateTable(output_workspace, table_name)
            arcpy.management.AddField(temp_match_table, "REL_OBJECTID", "LONG")
            arcpy.management.AddField(temp_match_table, "ATT_PATH", "TEXT", field_length=500)

            with arcpy.da.InsertCursor(temp_match_table, ["REL_OBJECTID", "ATT_PATH"]) as cursor:
                for att_group in attachment_groups:
                    parent_oid = att_group.get("parentObjectId")
                    infos = att_group.get("attachmentInfos", [])
                    if not infos:
                        continue

                    rel_folder = os.path.join(output_folder, f"{service_name}_attachments", str(parent_oid))
                    if not os.path.exists(rel_folder):
                        os.makedirs(rel_folder)
                        att_folders.append(rel_folder)

                    for info in infos:
                        att_id = info.get("id")
                        att_name = _safe_filename(info.get("name") or f"attachment_{att_id}")
                        att_url = f"{layer_url}/{parent_oid}/attachments/{att_id}"
                        params = {"f": "json"}
                        if token:
                            params["token"] = token

                        # Token is carried in query string for binary attachment download.
                        dl_url = att_url
                        if token:
                            dl_url = f"{att_url}?token={urllib.parse.quote(token)}"

                        out_file = os.path.join(rel_folder, att_name)
                        response = self.session.get(dl_url, timeout=120)
                        response.raise_for_status()
                        with open(out_file, "wb") as handle:
                            handle.write(response.content)
                        cursor.insertRow((parent_oid, out_file))

            arcpy.management.AddAttachments(
                final_fc,
                "OBJECTID",
                temp_match_table,
                "REL_OBJECTID",
                "ATT_PATH",
            )
            self._emit(f"Attachments added to {final_fc}")

            if self.clean_up_temp_attachments_data:
                for folder in att_folders:
                    try:
                        shutil.rmtree(folder)
                    except Exception as ex:
                        self._emit(f"Warning: Could not delete temporary attachment folder: {ex}", severity=1)
                try:
                    arcpy.management.Delete(temp_match_table)
                except Exception as ex:
                    self._emit(f"Warning: Could not delete temporary match table: {ex}", severity=1)
        except Exception as ex:
            self._emit(f"Warning: Could not download/add attachments: {ex}", severity=1)

    def make_service_name(self, service_info, output_workspace):
        max_path_length = 259
        if self.output_type == "Folder":
            max_path_length = 250

        workspace_len = len(output_workspace)
        max_name_len = max_path_length - workspace_len

        parent_id = ""
        service_name = service_info.get("name")
        service_id = str(service_info.get("id"))

        service_name_cl = service_name.encode("ascii", "ignore").decode("ascii")
        service_name_cl = arcpy.ValidateTableName(service_name_cl, output_workspace)
        service_name_cl = re.sub(r"_+", "_", service_name_cl).rstrip("_")

        if len(service_name_cl) > max_name_len:
            service_name_cl = service_name_cl[:max_name_len]

        if service_info.get("parentLayer"):
            parent_id = str(service_info.get("parentLayer").get("id"))

        if workspace_len + len(service_name_cl) > max_path_length:
            max_len = max_path_length - workspace_len
            if max_len < len(service_name_cl):
                service_name_cl = service_name_cl[:max_len]

        if service_name_cl not in self.service_output_name_tracking_list:
            self.service_output_name_tracking_list.append(service_name_cl)
        elif f"{service_name_cl}_{service_id}" not in self.service_output_name_tracking_list:
            service_name_cl = f"{service_name_cl}_{service_id}"
            self.service_output_name_tracking_list.append(service_name_cl)
        else:
            service_name_cl = f"{service_name_cl}{parent_id}_{service_id}"

        return service_name_cl

    def scrub_the_decks(self, fc_list):
        for fc in fc_list:
            try:
                arcpy.Delete_management(fc)
                self._emit(f"Deleted {fc}")
            except Exception as ex:
                self._emit(f"Warning: Could not delete {fc}: {ex}", severity=1)

    def pillage_the_layer(self, slyr, token, output_folder, output_workspace):
        try:
            downloaded_fc_list = []
            current_iter = 0
            final_fc = ""
            oid_count = 0
            slyr_start_time = datetime.datetime.today()

            self._emit(f"Now pillagin' yer data from {slyr}")

            json_param = {"f": "json"}
            if token:
                json_param["token"] = token
            service_info = self.execute_query(slyr, params=json_param)

            if service_info.get("error"):
                return f"Error: {service_info.get('error')}"

            service_info["serviceURL"] = slyr

            supports_json = True
            if self.strict_mode:
                supports_json = False
                supported = service_info.get("supportedQueryFormats")
                if supported:
                    for data_format in [f.strip() for f in supported.split(",")]:
                        if data_format == "JSON":
                            supports_json = True
                            break
                else:
                    self._emit("Strict mode scuttled, no supported formats, forgin' on", severity=1)

            objectid_field = "OBJECTID"
            field_list = service_info.get("fields")
            if field_list:
                for field in field_list:
                    if field.get("type") == "esriFieldTypeOID":
                        objectid_field = field.get("name")
                        break

            where_clause = self.query_str or "1=1"
            ct_params = {"where": where_clause, "returnCountOnly": "true", "f": "json"}
            if token:
                ct_params["token"] = token
            feature_count = self.execute_query(f"{slyr}/query", params=ct_params)
            service_info["FeatureCount"] = feature_count.get("count")

            service_name_cl = self.make_service_name(service_info, output_workspace)
            if self.output_type == "Folder":
                final_fc = os.path.join(output_workspace, f"{service_name_cl}.shp")
            else:
                final_fc = os.path.join(output_workspace, service_name_cl)

            if arcpy.Exists(final_fc) and not self.overwrite_output:
                return f"Skipped: {final_fc} exists and overwrite output is disabled"

            if self.write_service_info:
                info_file = os.path.join(output_folder, f"{service_name_cl}_info.txt")
                with open(info_file, "w") as i_file:
                    json.dump(service_info, i_file, sort_keys=True, indent=4, separators=(",", ": "))
                    self._emit(f"Yar! {service_name_cl} Service info stashed in '{info_file}'")

            if not supports_json:
                return "Failed: Service does not support JSON output"

            if self.query_str:
                where_clause = self.query_str
            else:
                where_clause = f"{objectid_field} > 0"

            oid_params = {
                "where": where_clause,
                "returnGeometry": "false",
                "returnIdsOnly": "true",
                "returnCountOnly": "false",
                "returnExtentOnly": "false",
                "f": "json",
            }
            if token:
                oid_params["token"] = token

            max_record_count = service_info.get("maxRecordCount") or self.sanity_max_record_count
            if max_record_count > self.sanity_max_record_count:
                self._emit(
                    f"{max_record_count} max records is a wee bit large, using {self.sanity_max_record_count} instead..."
                )
                max_record_count = self.sanity_max_record_count

            feature_oid_query = self.execute_query(f"{slyr}/query", params=oid_params)
            feature_oids = feature_oid_query.get("objectIds") if feature_oid_query else None

            if not feature_oids:
                if self.create_empty_schema:
                    self._create_empty_schema(final_fc, field_list, service_info)
                    return f"Success: Created empty feature class {final_fc}"
                raise DataPillagerError("Plunderin' failed: no feature OIDs returned")

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
                "f": "json",
            }

            oid_count = len(feature_oids)
            sortie_count = oid_count // max_record_count + (oid_count % max_record_count > 0)
            self._emit(f"{oid_count} records, in chunks of {max_record_count}, err, that be {sortie_count} sorties. Ready lads!")

            feature_oids.sort()
            for group in self.grouper(feature_oids, max_record_count):
                start_oid = group[0]
                end_oid = group[max_record_count - 1]
                if end_oid is None:
                    for value in reversed(group):
                        if value is not None:
                            end_oid = value
                            break

                if self.query_str:
                    where_clause = f"{self.query_str} AND {objectid_field} >= {start_oid} AND {objectid_field} <= {end_oid}"
                else:
                    where_clause = f"{objectid_field} >= {start_oid} AND {objectid_field} <= {end_oid}"

                params = feat_data_params_base.copy()
                params["where"] = where_clause
                if token:
                    params["token"] = token

                response = self.execute_query(f"{slyr}/query", params=params)
                features = response.get("features") if response else None
                if not features:
                    raise DataPillagerError("Abandon ship! Data access failed for one or more feature chunks")

                out_json_name = f"{service_name_cl}{current_iter}.json"
                out_json_file = os.path.join(output_folder, out_json_name)
                with codecs.open(out_json_file, "w", "utf-8") as out_file:
                    out_file.write(json.dumps(response, ensure_ascii=False))

                self._emit(f"Nabbed some json data fer ye: '{out_json_name}', oids {start_oid} to {end_oid}")

                if self.output_type == "Folder":
                    out_file_name = f"{service_name_cl}{current_iter}.shp"
                else:
                    out_file_name = f"{service_name_cl}{current_iter}"
                out_geofile = os.path.join(output_workspace, out_file_name)

                self._emit(f"Converting yer json to {out_geofile}")
                arcpy.JSONToFeatures_conversion(out_json_file, out_geofile)
                downloaded_fc_list.append(out_geofile)
                os.remove(out_json_file)
                current_iter += 1

            if downloaded_fc_list:
                self._emit(f"Stashin' all the booty in '{final_fc}'")
                self.combine_data(fc_list=downloaded_fc_list, output_fc=final_fc)

            if arcpy.Exists(final_fc):
                data_count = int(arcpy.GetCount_management(final_fc)[0])
                if data_count == oid_count:
                    self._emit("Scrubbing the decks...")
                    self.scrub_the_decks(downloaded_fc_list)
                else:
                    raise DataPillagerError(
                        f"Splicin' the data failed - found {data_count} but expected {oid_count}. Check {final_fc}."
                    )

            if self.include_attachments and feature_oids:
                self.get_attachments(slyr, final_fc, feature_oids, service_name_cl, output_folder, output_workspace, token)

            msg = f"{slyr} plundered to {final_fc} in {datetime.datetime.today() - slyr_start_time}"
            self._emit(msg)
            return f"Success: {msg}"
        except Exception as ex:
            self._emit(str(ex), severity=2)
            return f"Error: {ex}"

    def _create_empty_schema(self, final_fc, field_list, service_info):
        final_fc_name = os.path.basename(final_fc)
        self._emit(f"No OID values found, creating an empty {final_fc_name} with schema")

        esri_to_arcpy_geom = {
            "esriGeometryPoint": "POINT",
            "esriGeometryPolyline": "POLYLINE",
            "esriGeometryPolygon": "POLYGON",
        }
        geometry_type = esri_to_arcpy_geom.get(service_info.get("geometryType"), "POINT")

        spatial_ref = None
        extent = service_info.get("extent") or {}
        sr_info = extent.get("spatialReference") or {}
        if "wkid" in sr_info:
            spatial_ref = arcpy.SpatialReference(sr_info["wkid"])

        arcpy.CreateFeatureclass_management(self.output_workspace, final_fc_name, geometry_type, spatial_reference=spatial_ref)

        if field_list:
            for field in field_list:
                field_name = field.get("name")
                field_type = field.get("type")
                if field_type in ["esriFieldTypeOID", "esriFieldTypeGeometry"]:
                    continue

                arcpy_type = "TEXT"
                if field_type == "esriFieldTypeInteger":
                    arcpy_type = "LONG"
                elif field_type == "esriFieldTypeSmallInteger":
                    arcpy_type = "SHORT"
                elif field_type == "esriFieldTypeSingle":
                    arcpy_type = "FLOAT"
                elif field_type == "esriFieldTypeDouble":
                    arcpy_type = "DOUBLE"
                elif field_type == "esriFieldTypeDate":
                    arcpy_type = "DATE"

                field_length = field.get("length", 255) if arcpy_type == "TEXT" else None
                try:
                    arcpy.AddField_management(final_fc, field_name, arcpy_type, field_length=field_length)
                except Exception:
                    self._emit(f"Failed to add field: {field_name}", severity=1)

        self._emit(f"Created empty featureclass: {final_fc}")

    def run(self):
        start_time = datetime.datetime.today()

        self._emit(f"DataPillager core version: {CORE_VERSION}")
        self._emit(f"DataPillager core module: {__file__}")

        if not self.service_endpoint:
            raise DataPillagerError("Service endpoint is required")

        if not self.output_workspace:
            self.output_workspace = os.getcwd()

        token = ""

        try:
            if not os.path.exists(self.output_workspace):
                self._emit(f"Shiver me timbers, {self.output_workspace} doesn't exist! Trying to create it...")
                if self.output_workspace.endswith(".gdb"):
                    workspace_parent = os.path.dirname(self.output_workspace)
                    if workspace_parent and not os.path.exists(workspace_parent):
                        os.makedirs(workspace_parent)
                    arcpy.CreateFileGDB_management(workspace_parent, os.path.basename(self.output_workspace))
                elif self.output_workspace.endswith(".sde"):
                    raise DataPillagerError(
                        "Can't create an SDE workspace automatically. Create it and point the tool to it."
                    )
                else:
                    os.makedirs(self.output_workspace)

            output_desc = arcpy.Describe(self.output_workspace)
            self.output_type = output_desc.dataType
            output_folder = self.output_workspace if self.output_type == "Folder" else output_desc.path

            arcpy.env.overwriteOutput = self.overwrite_output
            if hasattr(arcpy.env, "preserveGlobalIds"):
                arcpy.env.preserveGlobalIds = self.preserve_global_ids

            adapter_name = self.get_adapter_name(self.service_endpoint)
            token_client_type = "requestip"
            if self.referring_domain:
                self.referring_domain = self.referring_domain.replace("http:", "https:")
                token_client_type = "referer"
            else:
                self.referring_domain = self.get_referring_domain(self.service_endpoint)
                if self.referring_domain == "https://www.arcgis.com":
                    token_client_type = "referer"

            self.session = self.create_session()

            if self.username and not self.existing_token:
                token = self.get_token(
                    referer=self.referring_domain,
                    adapter_name=adapter_name,
                    client_type=token_client_type,
                )
            elif self.existing_token:
                token = self.existing_token

            if self.include_attachments:
                self._emit(
                    "Arrr, ye be wantin' to plunder attachments too! Beware, it may take longer and use more storage."
                )

            self._emit(f"Start the plunder! {self.service_endpoint}")
            self._emit(f"We be stashing the booty in {self.output_workspace}")

            service_layers_to_get = self.get_all_the_layers(self.service_endpoint, token)
            self._emit(f"Blimey, {len(service_layers_to_get)} layers for the pillagin'")

            slyr_tracker = {}
            for slyr in service_layers_to_get:
                slyr_tracker[slyr] = self.pillage_the_layer(slyr, token, output_folder, self.output_workspace)

            for slyr, result in slyr_tracker.items():
                self._emit(f"{slyr} plunder result: {result}")

            return slyr_tracker
        finally:
            if self.user_overwrite_setting is not None:
                arcpy.env.overwriteOutput = self.user_overwrite_setting
            if hasattr(arcpy.env, "preserveGlobalIds") and self.user_preserve_globalids_setting is not None:
                arcpy.env.preserveGlobalIds = self.user_preserve_globalids_setting
            if self.session is not None:
                self.session.close()
            self._emit(f"Plunderin' done, in {datetime.datetime.today() - start_time}")
