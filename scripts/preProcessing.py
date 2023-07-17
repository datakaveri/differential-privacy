import json
import jsonschema
import pandas as pd
from pandas import json_normalize

def schemaValidator(schemaFile, configFile):
    schemaFile = '../config/' + schemaFile
    configFile = '../config/' + configFile

# Load the JSON schema
    with open(schemaFile, "r") as f:
        schema = json.load(f)

# Load the JSON document to validate
    with open(configFile, "r") as f:
        document = json.load(f)

# Validate the document against the schema
    jsonschema.validate(instance=document, schema=schema)
    return

# def readFile(configFileName):
#     #reading config
#     configFile = '../config/' + configFileName
#     with open(configFile, "r") as cfile:
#         configDict = json.load(cfile)
#     dataFileName = '../data/' + configDict['dataFile']
        
#     with open(dataFileName, "r") as dfile:
#         dataDict = json.load(dfile)        
#     #loading data
#     dataframe = pd.json_normalize(dataDict)
        
#     pd.set_option('mode.chained_assignment', None)
#     print('The loaded file is: ' + dataFileName + ' with shape ' + str(dataframe.shape))
    
#     return dataframe, configDict

def dropDuplicates(dataframe, configDict):

    #dropping duplicates based on config file parameters
    if(len(configDict['duplicateDetection']))==0:
        dfLen1 = len(dataframe)
        dfDrop = dataframe.drop_duplicates(inplace = False, ignore_index = True)
        dfLen2 = len(dfDrop)
        dupeCount = dfLen1 - dfLen2
        print("\nIdentifying and removing duplicates...")
        print(str(dupeCount) + ' duplicate rows have been removed.') 
        print(str(dfDrop.shape) + ' is the shape of the deduplicated dataframe .')
        dataframe = dfDrop  
    else:
        dfLen1 = len(dataframe)
        dfDrop = dataframe.drop_duplicates(subset = configDict['duplicateDetection'], inplace = False, ignore_index = True)
        dfLen2 = len(dfDrop)
        dupeCount = dfLen1 - dfLen2
        print("\nIdentifying and removing duplicates...")
        print(str(dupeCount) + ' duplicate rows have been removed.') 
        print(str(dfDrop.shape) + ' is the shape of the deduplicated dataframe .')
        dataframe = dfDrop  
    return dataframe

def suppress(dataframe, configDict):
    dataframe = dataframe.drop(columns = configDict['suppressCols'])
    print("\nDropping columns from configuration file...")
    print(str(dataframe.shape) + ' is the shape of the dataframe after suppression.\n\nThe number of unique rows are:\n' + str(dataframe.shape[0]))
    return dataframe
