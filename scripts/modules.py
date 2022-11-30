# File created to store modules for Differential Privacy
# Modules:
# 1. Filter/Suppress
# 2. Pseudonymization
# 3. Generalization
# 4. Aggregation
# 5. Differential Privacy (noise addition)
import pandas as pd
import numpy as np
import json
from pandas.io.json import json_normalize

def readFile(configFileName):
    #reading config
    configFile = 'config/' + configFileName
    with open(configFile, "r") as cfile:
        configDict = json.load(cfile)
    #reading datafile
    dataFileName = 'data/' + configDict['dataFile']
    with open(dataFileName, "r") as dfile:
        dataDict = json.load(dfile)
    #loading data
    dataframe = pd.json_normalize(dataDict)
    pd.set_option('mode.chained_assignment', None)
    print('The loaded file is: ' + dataFileName)
    
    #dropping duplicates based on config file parameters
    dupe1 = configDict['duplicateDetection'][0]
    dupe2 = configDict['duplicateDetection'][1]
    dfLen1 = len(dataframe)
    dfDrop = dataframe.drop_duplicates(subset = [dupe1, dupe2], inplace = False, ignore_index = True)
    dfLen2 = len(dfDrop)
    dupeCount = dfLen1 - dfLen2
    p1 = print(str(dupeCount) + ' duplicate rows have been removed.') 
    p2 = print(str(dfDrop.shape) + ' is the shape of the new dataframe.')
    dataframe = dfDrop  
    return dataframe

def suppress(dataframe):
    dataframe = dataframe.drop(columns = configDict['suppressCols'])
    print("Dropping columns from configuration file...")
    print("The shape of the new dataframe is:")
    print(dataframe.shape)
    return dataframe
