import pandas as pd
import numpy as np
import json
import jsonschema
import preProcessing as premod
import postProcessing as postmod
from pandas import json_normalize
def chunkHandling():
    file_names_list = ['../data/split_file_0.json',
              '../data/split_file_1.json',
              '../data/split_file_2.json',
              '../data/split_file_3.json',
              '../data/split_file_4.json',
              '../data/split_file_5.json',
              '../data/split_file_6.json',
              '../data/split_file_7.json',
              '../data/split_file_8.json',
              '../data/split_file_9.json']
    configFileName = '../config/SEAggConfig.json'
    with open(configFileName, "r") as cfile:
        configDict = json.load(cfile)
    schemaFileName = 'SEAggSchema.json'
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
            dataframe= genAggGeneralization(dataframe,configDict)
            dfGrouped_dict=resolutionCount(dataframe)
            dfFinalGrouped_dict = dfGrouped_dict  
            # print(file)
        elif (len(lengthList) > 1):
            dataframe = genAggGeneralization(dataframe,configDict)
            dfGrouped_dict=resolutionCount(dataframe)
            dfFinalGrouped_dict=merge(dfFinalGrouped_dict,dfGrouped_dict)        

        # print('########################################################################################')
        print('The length of the grouped dataframe is ',len(dfFinalGrouped_dict), 'for chunk ',len(lengthList))
       # print('No. of Unique HATs of the grouped dataframe is: ', dfFinalGrouped['HAT'].nunique())
       # print('The number of unique license plates in the grouped dataframe is: ', dfFinalGrouped['license_plate'].nunique())
        print('########################################################################################')
        # dataframe, dfSensitivity, dfCount = stmod.chunkedAggregator(dataframe, configDict)
    return dfFinalGrouped_dict
def genAggGeneralization(dataframe, configFile):
    #separating year and month and creating aggregate year_month column
    year = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.year
    month = pd.to_datetime(dataframe[configFile["datetimeCol"]]).dt.month
    dataframe['yearMonth'] = year.astype(str) + '_' + month.astype(str)

    # create a ward + year month generalization
    # WAYM = Ward At a Year Month
    dataframe['WAYM'] = dataframe['yearMonth'] + ' ' + dataframe['wardID'].astype(str) 
    return dataframe
def resolutionCount(dataframe):
    WAYMD_dict = {}
    for WAYM, group in dataframe.groupby('WAYM'):
        df = group.pivot_table(index='department', columns='resolutionStatus', aggfunc='size', fill_value=0)
        WAYMD_dict[WAYM] = df
    return WAYMD_dict
def merge(dict1,dict2):
    print('###############Merge Started#################')
    for WAYM, df in dict2.items():
        if WAYM in dict1:
            df1=dict1[WAYM]
            combined_df = df1.combine_first(df)
            dict1[WAYM] = combined_df
        else:
            dict1[WAYM] = df
    print('###############Merge Ended################')
    return dict1

data_dict=chunkHandling()
postmod.outputFileGenAgg(data_dict)