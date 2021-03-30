# DataPillager
![Logo](DataPillagerIcon.png)

![GitHub all releases](https://img.shields.io/github/downloads/gdherbert/DataPillager/total)

Most GIS people have had a need to download vector data at some point. What do you do if there isn't a handy package to download, but you have access to a REST service containing the data?

## Enter the DataPillager. ##
A Python script to download data from Esri REST services (ArcGIS Server, ArcGIS Online).
Accepts a username and password for secured services, and has an experimental feature where you can enter a valid token instead. Includes a highly experimental query option as well. 

Some of the useful features include:  
* Can download child services, just supply the parent URL.  
* Handles super long service names.  
* Outputs a text file containing service data for metadata purposes  

*Notes*
* Does not download map services that do not have a json feature representation.  
* Does not download tables.  
* Downloading to a filegeodatabase or enterprise geodatabase is recommended over a folder to maintain field names etc.  
* The ArcGIS Desktop version generates a layer file from the service symbology. This is not yet implemented in the Pro version.  

## How to use  ##
* Clone the repo or download the zip file for the release you want. If in doubt download the latest zip from the main branch.   
* Open the supplied toolbox in Esri software and check that the tool is pointed correctly to the script location (reconnect the script source is needed). Import the script if you want to be able to easily move it around. 
*  Run the script, and enter the URL to an Esri service, and the destination (filegeodatabase recommended).

### What about ArcGIS Desktop? ###
The older version of this tool (to 1.3) supports ArcGIS Desktop, version 2.0 onwards supports Pro. For conevenience, the /Desktop subfolder contains the v1.3 ArcGIS Desktop toolbox and Python 2.7 script. You can also download release v1.3, in the DesktopPython2 branch to only get the ArcGIS Desktop version.  

### Requirements ###
The Esri Arcpy library used requires a licensed install of Esri ArcGIS Pro 2.7 or above (or ArcGIS Desktop 10.5 or above for v1.3).

**IMPORTANT**

The main toolbox in this repo has switched to Python 3 and ArcGIS Pro with the v2.0. release (March 2021).

No further Desktop or Python 2 development is anticipated with Esri stopping desktop releases. The Desktop version should remain compatible with any DDesktop releases past 10.8.1 as long as they continue to use Python 2. 

