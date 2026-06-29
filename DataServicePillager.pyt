# -*- coding: utf-8 -*-
"""DataPillager Python Toolbox for ArcGIS Pro."""

import os
import urllib.parse

import arcpy

from datapillager_core import DataPillagerError, DataPillagerRunner


class Toolbox(object):
    def __init__(self):
        self.label = "Data Service Pillager"
        self.alias = "datapillager"
        self.tools = [DataServicePillagerTool]


class DataServicePillagerTool(object):
    def __init__(self):
        self.label = "Data Service Pillager"
        self.description = "Yaar! Like a pirate on the seven seas, this tool will rampantly pillage data from an ArcGIS Service. Handles multiple layers in a service, or just one.\nPass in optional authentication to sneak past the guards!"
        self.canRunInBackground = False

    def getParameterInfo(self):
        params = []

        p0 = arcpy.Parameter(
            displayName="Service Endpoint",
            name="service_endpoint",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )

        p1 = arcpy.Parameter(
            displayName="Output Workspace (Folder, GDB or SDE)",
            name="output_workspace",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input",
        )

        p2 = arcpy.Parameter(
            displayName="Max Retries",
            name="max_tries",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
        )
        p2.value = 5
        p2.filter.type = "Range"
        p2.filter.list = [1, 100]
        p2.controlCLSID = "{C8C46E43-3D27-4485-9B38-A49F3AC588D9}"

        p3 = arcpy.Parameter(
            displayName="Retry Backoff Factor",
            name="sleep_time",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
        )
        p3.value = 2
        p3.filter.type = "Range"
        p3.filter.list = [1, 100]

        p4 = arcpy.Parameter(
            displayName="Strict Mode (Require JSON Query Support)",
            name="strict_mode",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
        )
        p4.value = True

        p5 = arcpy.Parameter(
            displayName="Username",
            name="username",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )

        p6 = arcpy.Parameter(
            displayName="Password",
            name="password",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )

        p7 = arcpy.Parameter(
            displayName="Referring Domain",
            name="referring_domain",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )

        p8 = arcpy.Parameter(
            displayName="Existing Token",
            name="existing_token",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )

        p9 = arcpy.Parameter(
            displayName="SQL Query (Where Clause)",
            name="query_str",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )

        p10 = arcpy.Parameter(
            displayName="Ignore SSL Verification",
            name="ignore_ssl_verification",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
        )
        p10.value = True

        p11 = arcpy.Parameter(
            displayName="CA Bundle Path",
            name="ca_bundle_path",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input",
        )

        p12 = arcpy.Parameter(
            displayName="Create Empty Schema If No Rows",
            name="create_empty_schema",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
        )
        p12.value = False

        p13 = arcpy.Parameter(
            displayName="Overwrite Output",
            name="overwrite_output",
            datatype="GPBoolean",
            parameterType="Required",
            direction="Input",
        )
        p13.value = True

        p14 = arcpy.Parameter(
            displayName="Include Attachments",
            name="include_attachments",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
        )
        p14.value = False

        p15 = arcpy.Parameter(
            displayName="Clean Up Temporary Attachment Files",
            name="clean_up_temp_attachments_data",
            datatype="GPBoolean",
            parameterType="Optional",
            direction="Input",
        )
        p15.value = False

        params.extend([p0, p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15])
        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        ignore_ssl = bool(parameters[10].value) if parameters[10].value is not None else True
        include_attachments = bool(parameters[14].value) if parameters[14].value is not None else False
        has_token = bool(parameters[8].valueAsText)

        parameters[11].enabled = not ignore_ssl
        parameters[15].enabled = include_attachments

        if has_token:
            parameters[5].enabled = False
            parameters[6].enabled = False
        else:
            parameters[5].enabled = True
            parameters[6].enabled = True

    def updateMessages(self, parameters):
        service_endpoint = (parameters[0].valueAsText or "").strip()
        output_workspace = (parameters[1].valueAsText or "").strip()

        max_tries = parameters[2].value
        sleep_time = parameters[3].value

        username = (parameters[5].valueAsText or "").strip()
        password = (parameters[6].valueAsText or "").strip()
        existing_token = (parameters[8].valueAsText or "").strip()

        ignore_ssl = bool(parameters[10].value) if parameters[10].value is not None else True
        ca_bundle_path = (parameters[11].valueAsText or "").strip()

        query_str = (parameters[9].valueAsText or "").strip()

        include_attachments = bool(parameters[14].value) if parameters[14].value is not None else False
        clean_up_attachments = bool(parameters[15].value) if parameters[15].value is not None else False

        if service_endpoint:
            parsed = urllib.parse.urlparse(service_endpoint)
            if parsed.scheme.lower() not in ("http", "https"):
                parameters[0].setErrorMessage("Service endpoint must start with http:// or https://")

        if max_tries is not None and int(max_tries) < 1:
            parameters[2].setErrorMessage("Max Retries must be >= 1")

        if sleep_time is not None and int(sleep_time) < 0:
            parameters[3].setErrorMessage("Retry Backoff Factor must be >= 0")

        if existing_token and (username or password):
            parameters[8].setWarningMessage("Existing token is provided; username/password will be ignored")

        if (username and not password) or (password and not username):
            parameters[6].setErrorMessage("Provide both username and password, or provide an existing token")

        if username and password and service_endpoint and "arcgis.com" not in service_endpoint.lower():
            if not (parameters[7].valueAsText or "").strip():
                parameters[7].setWarningMessage(
                    "Using username/password on non-arcgis.com services often requires Referring Domain. "
                    "Set it to your ArcGIS Server URL or Portal URL depending on your authentication configuration."
                )

        if not ignore_ssl and ca_bundle_path and not os.path.isfile(ca_bundle_path):
            parameters[11].setErrorMessage("CA bundle path must point to an existing file")

        if clean_up_attachments and not include_attachments:
            parameters[15].setErrorMessage("Cleanup can only be enabled when Include Attachments is true")

        if output_workspace and include_attachments:
            lower_path = output_workspace.lower()
            # Attachment support requires geodatabase output.
            if not (lower_path.endswith(".sde") or lower_path.endswith(".gdb")):
                parameters[14].setErrorMessage("Include Attachments requires a file or sde geodatabase output workspace")

        if query_str and "%25" in query_str:
            parameters[9].setWarningMessage("Query appears pre-encoded; enter a plain SQL where clause")

    def execute(self, parameters, messages):
        def emit(message, severity=0):
            if severity == 0:
                arcpy.AddMessage(message)
            elif severity == 1:
                arcpy.AddWarning(message)
            else:
                arcpy.AddError(message)

        config = {
            "service_endpoint": parameters[0].valueAsText,
            "output_workspace": parameters[1].valueAsText,
            "max_tries": parameters[2].value,
            "sleep_time": parameters[3].value,
            "strict_mode": parameters[4].value,
            "username": parameters[5].valueAsText,
            "password": parameters[6].valueAsText,
            "referring_domain": parameters[7].valueAsText,
            "existing_token": parameters[8].valueAsText,
            "query_str": parameters[9].valueAsText,
            "ignore_ssl_verification": parameters[10].value,
            "ca_bundle_path": parameters[11].valueAsText,
            "create_empty_schema": parameters[12].value,
            "overwrite_output": parameters[13].value,
            "include_attachments": parameters[14].value,
            "clean_up_temp_attachments_data": parameters[15].value,
        }

        try:
            runner = DataPillagerRunner(config=config, message_handler=emit)
            runner.run()
        except DataPillagerError as ex:
            arcpy.AddError(str(ex))
            raise arcpy.ExecuteError
        except Exception as ex:
            arcpy.AddError(f"Unexpected failure: {ex}")
            raise arcpy.ExecuteError
