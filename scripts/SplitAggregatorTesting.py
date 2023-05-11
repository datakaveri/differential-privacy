import preProcessing as premod
import pandas as pd
import os
import json
import spatioTemporalModules as stmod  
import numpy as np
import h3

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

file_names = ['../data/suratITMSDPtest.json']

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
    lengthList.append(file)
    with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            #loading data
            dataframe = pd.json_normalize(dataDict)
     
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
        dfCombined = pd.concat([dfGrouped, dfCombined],  ignore_index=True)
        dfCombined.drop_duplicates(subset = ['HAT', 'Date', 'license_plate'], inplace = True)
        print(file)
        print('dfCombined', dfCombined)

        dfGroupedCombined = dfCombined.groupby(['HAT','Date','license_plate']).agg({
                                                    'count': 'sum',
                                                    'sum': 'sum',
                                                    'max':'max',
                                                    'min':'min'}).reset_index()
        dfFinalGrouped = dfGroupedCombined
        # dfFinalGrouped = dfCombined
print('dfFinalGrouped before filtering', dfFinalGrouped)
# print('dfFinal Grouped and combined before filtering', dfFinalGroupedCombined)

print('########################################################################################')
print('The length of the grouped dataframe is: ', len(dfFinalGrouped))
print('No. of Unique HATs of the grouped dataframe is: ', dfFinalGrouped['HAT'].nunique())
print('The number of unique license plates in the grouped dataframe is: ', dfFinalGrouped['license_plate'].nunique())
print('########################################################################################')


#filtering average num of occurences per day per HAT
date = dfFinalGrouped["Date"].unique()
minEventOccurencesPerDay = int(configDict["minEventOccurences"])
#making the limit independent of date since we are looking at the dataframe on a per day basis
limit = minEventOccurencesPerDay
dfFiltered = dfFinalGrouped[dfFinalGrouped['count'] >= limit]
dfFiltered = dfFinalGrouped["HAT"].isin(dfFiltered["HAT"])
dfFinalGrouped = dfFinalGrouped[dfFiltered]

print('Number of unique HATs left after filtering is: ' + str(dfFinalGrouped['HAT'].nunique()))
print('########################################################################################')

print('dfFinal Grouped after filtering', dfFinalGrouped)

