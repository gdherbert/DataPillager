# -*- coding: utf-8 -*-
"""Legacy script entrypoint for DataPillager.

The canonical implementation now lives in datapillager_core.py and the
Python toolbox in DataServicePillager.pyt.
"""

import arcpy

from datapillager_core import DataPillagerRunner


def _emit_tool_message(message, severity=0):
    if severity == 0:
        arcpy.AddMessage(message)
    elif severity == 1:
        arcpy.AddWarning(message)
    else:
        arcpy.AddError(message)


def main():
    config = {
        "service_endpoint": arcpy.GetParameterAsText(0),
        "output_workspace": arcpy.GetParameterAsText(1),
        "max_tries": arcpy.GetParameter(2),
        "sleep_time": arcpy.GetParameter(3),
        "strict_mode": arcpy.GetParameter(4),
        "username": arcpy.GetParameterAsText(5),
        "password": arcpy.GetParameterAsText(6),
        "referring_domain": arcpy.GetParameterAsText(7),
        "existing_token": arcpy.GetParameterAsText(8),
        "query_str": arcpy.GetParameterAsText(9),
        "enforce_ssl_verification": arcpy.GetParameter(10),
        "ca_bundle_path": arcpy.GetParameterAsText(11),
        "create_empty_schema": arcpy.GetParameter(12),
        "overwrite_output": arcpy.GetParameter(13),
        "preserve_global_ids": arcpy.GetParameter(14),
        "write_service_info": arcpy.GetParameter(15),
        "include_attachments": arcpy.GetParameter(16),
        "clean_up_temp_attachments_data": arcpy.GetParameter(17),
    }

    runner = DataPillagerRunner(config=config, message_handler=_emit_tool_message)
    runner.run()


if __name__ == '__main__':
    main()
