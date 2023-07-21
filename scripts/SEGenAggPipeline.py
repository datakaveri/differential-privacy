import pandas as pd
import numpy as np
import json
import jsonschema
import preProcessing as premod
import postProcessing as postmod
import GenAggModules as  gamod
from pandas import json_normalize
def chunkHandling():
    with open("../config/configFile.json", "r") as config_file:
        file_names_list = json.load(config_file)
    configFileName = '../config/SEGenAggConfig.json'
    with open(configFileName, "r") as cfile:
        configDict = json.load(cfile)
    schemaFileName = 'SEGenAggSchema.json'
    print('\n####################################################################\n')
    print('PREPROCESSING')
    lengthList = []
    dfFinalGrouped = pd.DataFrame()

    #groupByCol = configDict['groupByCol']

    for file in file_names_list:

        lengthList.append(file)
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframe = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframe.shape))
            configDict['dataFile'] = file

        premod.schemaValidator(schemaFileName, configFileName)

        #reading the file and dropping any duplicates
        # dataframe, configDict = premod.readFile(configFileName)
        #dropping duplicates
        dataframe = premod.dropDuplicates(dataframe, configDict)

        #supressing any columns that may not be required in the final output
        dataframe = premod.suppress(dataframe, configDict)        
        #aggregating dataframe
        # print('########################################################################################')
        print('The chunk number is: ', (len(lengthList)))
        if len(lengthList) == 1:
            dataframe= gamod.genAggGeneralization(dataframe,configDict)
            dfGrouped_dict=gamod.resolutionCount(dataframe)
            dfFinalGrouped_dict = dfGrouped_dict  
            # print(file)
        elif (len(lengthList) > 1):
            dataframe = gamod.genAggGeneralization(dataframe,configDict)
            dfGrouped_dict=gamod.resolutionCount(dataframe)
            dfFinalGrouped_dict=gamod.merge(dfFinalGrouped_dict,dfGrouped_dict)        

        # print('########################################################################################')
        print('The length of the grouped dataframe is ',len(dfFinalGrouped_dict), 'for chunk ',len(lengthList))
       # print('No. of Unique HATs of the grouped dataframe is: ', dfFinalGrouped['HAT'].nunique())
       # print('The number of unique license plates in the grouped dataframe is: ', dfFinalGrouped['license_plate'].nunique())
        print('########################################################################################')
        # dataframe, dfSensitivity, dfCount = stmod.chunkedAggregator(dataframe, configDict)
    return dfFinalGrouped_dict

data_dict=chunkHandling()
postmod.outputFileGenAgg(data_dict)