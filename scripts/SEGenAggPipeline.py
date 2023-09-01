import pandas as pd
import numpy as np
import json
#import jsonschema
import preProcessing as premod
import postProcessing as postmod
import genAggModules as  gamod
from pandas import json_normalize

def chunkHandling(configDict):
    with open("../config/genAggFNList.json", "r") as config_file:
            file_names_list = json.load(config_file)
   
    configDict = configDict['genAgg']

    print('\n####################################################################\n')
    print('PREPROCESSING')
    lengthList = []

    #groupByCol = configDict['groupByCol']

    for file in file_names_list:

        lengthList.append(file)
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframe = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframe.shape))
            configDict['dataFile'] = file      

        #reading the file and dropping any duplicates
        # dataframe, configDict = premod.readFile(configFileName)
        #dropping duplicates
        dataframe['comments']=dataframe['comments'].astype(str)
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

def main(configDict):
    data_dict=chunkHandling(configDict)
    postmod.outputFileGenAgg(data_dict)
    
if __name__ == "__main__":
    main()
    