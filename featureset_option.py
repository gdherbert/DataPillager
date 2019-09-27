# interesting approach to saving data from a service using arcpy.FeatureSet()
# load the featuresets into a dictionary, then loop through the dict and merge them

# Gather features
fs = dict()
# download features from REST
for each set_of_features:
  # get json
  urlstring = baseURL + "/query?where={}&returnGeometry=true&outFields={}&f=json".format(where,fields)
  fs[i] = arcpy.FeatureSet()
  fs[i].load(urlstring)

# Save features
fslist = []
for key,value in fs.items():
  fslist.append(value)
arcpy.Merge_management(fslist, outdata)