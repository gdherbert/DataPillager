# DataPillager
![Logo](DataPillagerIcon.png)

Most GIS people have had a need to download vector data at some point. What do you do if there isn't a handy package to download, but you have access to a REST service containing the data?

Enter the DataPillager.
A Python script to download data from Esri REST services (ArcGIS Server, ArcGIS Online).
Accepts a username and password for secured services, and has an experimental feature where you can enter a valid token instead. Includes a highly experimental query option as well. 

Some of the useful features include:
   Can download child services, just supply the parent URL.
   Handles super long service names
   Outputs a text file containing service data for metadata purposes

Does not download map services that do not have a json feature representation
While it can download to a folder, filegeodatabase or enterprise geodatabase, a file or enterprise geodatabase is recommended.

Designed to run from an Esri toolbox (supplied).
You may need to reconnect the script source from the toolbox as it is not imported.
The /Desktop subfolder contains an ArcGIS Desktop toolbox and Python 2.7 script. You can also download release v1.3, in the DesktopPython2 branch to only get the ArcGIS Desktop version.
    THe ArcGIS Desktop version generates a layer file from the service symbology


Requires a licensed install of Esri ArcGIS Pro 2.7 or ArcGIS Desktop 10.5 or above (for the arcpy dependency).

**IMPORTANT**

The main toolbox in this repo has switched to Python 3 and ArcGIS Pro with the v2.0. release (March 2021).

No further Desktop/Python 2 development is anticipated with Esri stopping desktop releases at 10.8.1. 

