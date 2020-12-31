# DataPillager
![Logo](DataPillagerIcon.png)

Most GIS people have had a need to download vector data at some point. What do you do if there isn't a handy package to download, but you have access to a REST service containing the data?

Enter the DataPillager.
A Python script to download data from Esri REST services (ArcGIS Server, ArcGIS Online).
Accepts a username and password for secured services, and has an experimental feature where you can enter a valid token instead.

Designed to run from an Esri toolbox (supplied).
You may need to reconnect the script source from the toolbox as it is not imported.

Requires a licensed install of Esri ArcGIS Pro 2.7 or above (for the arcpy dependency).

**IMPORTANT**

This repo is switching to Python 3 and ArcGIS Pro in 2021.

No further Desktop/Python 2 development is anticipated with Esri stopping desktop releases at 10.8.1. 

If you require the ArcGIS Desktop Python 2.X version, please download release v1.3, in the DesktopPython2 branch. That code is the ArcGIS Desktop version which works with ArcGIS 10.3+.

The v2.0 release will be a breaking release, and will only support Python 3.