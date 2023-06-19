import preProcessing as premod
import pandas as pd
import os
import json
import spatioTemporalModules as stmod  
import numpy as np
import h3
# importing the datetime package  
import datetime  

# creating a list of file names

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


# file_names_list = ['../data/file-1.json',
#               '../data/file-2.json',
#               '../data/file-3.json',]

# file_names = ['../data/suratITMSDPtest.json']

configFile = '../config/DPConfigITMS.json'
with open(configFile, "r") as cfile:
    configDict = json.load(cfile)
    # genType = configDict['genType']
    configDict = configDict['spatio-temporal']
    # print(configDict['suppressCols'])
groupByCol = configDict['groupByCol']

lengthList = []
dfCombined = pd.DataFrame()


for file in file_names_list:
# for file in file_names:  
# //TODO call next file_name
    lengthList.append(file)
    with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            # dataDict = [json.loads(line) for line in open(file, 'r')]

            #loading data
            dataframe = pd.json_normalize(dataDict)
     
    #epoch time to datetime
    # dataframe['observationDateTime'] = pd.to_datetime(dataframe['observationDateTime'], unit='ms')
    print(dataframe.head())

    #drop dupes
    dataframe = premod.dropDuplicates(dataframe, configDict)

    #supression
    dataframe = dataframe.drop(columns = configDict['suppressCols'])

    #generalization
    dataframe = stmod.spatioTemporalGeneralization(dataframe,configDict)

    print('The length of the list at this stage is: ', (len(lengthList)))
    print('########################################################################################')
    if len(lengthList) == 1:
        dfGrouped = dataframe.groupby(['HAT','Date','license_plate']).agg(
                                count=(groupByCol,'count'),
                                sum=(groupByCol,'sum'),
                                max=(groupByCol,'max'),
                                min=(groupByCol,'min')).reset_index()
                  
        dfFinalGrouped = dfGrouped  
        print(file)
    elif (len(lengthList) > 1):
        dfGrouped = dataframe.groupby(['HAT','Date','license_plate']).agg(
                                count=(groupByCol,'count'),
                                sum=(groupByCol,'sum'),
                                max=(groupByCol,'max'),
                                min=(groupByCol,'min')).reset_index()
                
        dfGrouped = pd.concat([dfGrouped, dfFinalGrouped],  ignore_index=True)
        print(file)
        print('dfGrouped')
        print(dfGrouped)

        dfGroupedCombined = dfGrouped.groupby(['HAT','Date','license_plate']).agg({
                                                    'count': 'sum',
                                                    'sum': 'sum',
                                                    'max':'max',
                                                    'min':'min'}).reset_index()
        dfFinalGrouped = dfGroupedCombined
dfFinalGrouped['mean'] = np.round((dfFinalGrouped['sum']/dfFinalGrouped['count']), 2)
print('')
print('dfFinalGrouped before filtering')
print(dfFinalGrouped) 

print('########################################################################################')
print('The length of the grouped dataframe is: ', len(dfFinalGrouped))
print('No. of Unique HATs of the grouped dataframe is: ', dfFinalGrouped['HAT'].nunique())
print('The number of unique license plates in the grouped dataframe is: ', dfFinalGrouped['license_plate'].nunique())
print('########################################################################################')


#filtering average num of occurences per day per HAT
date = dfFinalGrouped["Date"].unique()
minEventOccurencesPerDay = int(configDict["minEventOccurences"])
limit = len(date) * minEventOccurencesPerDay
dfFiltered = dfFinalGrouped.groupby(['HAT', 'Date']).agg({'license_plate':'nunique'}).reset_index()
# //TODO license_plate to replaced with generic input from the config file
dfFiltered = dfFiltered.groupby(['HAT']).agg({'license_plate':'sum'}).reset_index()
dfFiltered.rename(columns={"license_plate": "license_plate_count"}, inplace=True)
dfFiltered = dfFiltered[dfFiltered['license_plate_count'] >= limit]
dfFiltered = dfFinalGrouped["HAT"].isin(dfFiltered["HAT"])
dfFinalGrouped = dfFinalGrouped[dfFiltered]
# dfFinalGrouped.to_csv('groupingTestMultiple.csv')
print('Number of unique HATs left after filtering is: ' + str(dfFinalGrouped['HAT'].nunique()))
print('########################################################################################')

print('dfFinal Grouped after filtering', dfFinalGrouped)

