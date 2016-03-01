# DataPillager
![Logo](DataPillagerIcon.png)

Most GIS people have had a need to download data at some point. What do you do if there isn't a handy package to download, but you have access to a REST service containing the data?

Enter the DataPillager.
A Python script to download data from Esri REST services (ArcGIS Server, ArcGIS Online).
Accepts a username and password for secured services, and has an experimental feature where you can enter a valid token instead.

Designed to run from an Arc toolbox (supplied). Toolbox versions for ArcGIS 10.1 and 10.3 are included.
You may need to reconnect the script source from the toolbox as it is not imported.

Requires a licensed install of Esri ArcGIS 10.2 or above (for the arcpy dependency).
