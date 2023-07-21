import spatioTemporalModules as stmod
import categoricalModules as cmod
import preProcessing as premod
import postProcessing as postmod
import json
import pandas as pd
import numpy as np


# //TODO: Write a wrapper function to ensure that the correct pipeline file is run
# //TODO: Include file names list handling within the wrapper file
# //TODO: Handle data tapping within the individual pipeline files
'''file_names_list = ['../data/split_file_0.json',
            '../data/split_file_1.json',
            '../data/split_file_2.json',
            '../data/split_file_3.json',
            '../data/split_file_4.json',
            '../data/split_file_5.json',
            '../data/split_file_6.json',
            '../data/split_file_7.json',
            '../data/split_file_8.json',
            '../data/split_file_9.json']'''

def chunkHandling(config, schema, fileList, dataTapChoice):
    with open(config, "r") as cfile:
        configDict = json.load(cfile)
    schemaFileName = schema

    print('\n####################################################################\n')
    print('PREPROCESSING')
    lengthList = []
    dfFinalGrouped = pd.DataFrame()
    groupByCol = configDict['groupByCol']

    for file in fileList:
        lengthList.append(file)
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframe = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframe.shape))
            configDict['dataFile'] == file

        #dropping duplicates
        dataframe = premod.dropDuplicates(dataframe, configDict)

        #supressing any columns that may not be required in the final output
        dataframe = premod.suppress(dataframe, configDict)

        #generalization applied to categories like time, location, etc that may contain personal identifiable information
        dataframe = stmod.spatioTemporalGeneralization(dataframe, configDict)
        
        if dataTapChoice == 1:
            #aggregating dataframe for data tapping option 1
            print('The chunk number is: ', (len(lengthList)))
            if len(lengthList) == 1:
                dfGrouped = dataframe.groupby(['Date','license_plate','HAT']).agg(
                                        count=(groupByCol,'count'),
                                        sum=(groupByCol,'sum'),
                                        max=(groupByCol,'max'),
                                        min=(groupByCol,'min')).reset_index()
                        
                dfFinalGrouped = dfGrouped  
                # print(file)
            elif (len(lengthList) > 1):
                dfGrouped = dataframe.groupby(['Date','license_plate','HAT']).agg(
                                        count=(groupByCol,'count'),
                                        sum=(groupByCol,'sum'),
                                        max=(groupByCol,'max'),
                                        min=(groupByCol,'min')).reset_index()
                        
                dfGrouped = pd.concat([dfGrouped, dfFinalGrouped],  ignore_index=True)

                dfGroupedCombined = dfGrouped.groupby(['Date','license_plate','HAT']).agg({
                                                            'count': 'sum',
                                                            'sum': 'sum',
                                                            'max':'max',
                                                            'min':'min'}).reset_index()
                dfFinalGrouped = dfGroupedCombined
            dfFinalGrouped['mean'] = np.round((dfFinalGrouped['sum']/dfFinalGrouped['count']), 2)


            # print('')
            # print('dfFinalGrouped before filtering')
            # print(dfFinalGrouped) 

            # print('########################################################################################')
            print('The length of the grouped dataframe is ',len(dfFinalGrouped), 'for chunk ',len(lengthList))
            print('No. of Unique HATs of the grouped dataframe is: ', dfFinalGrouped['HAT'].nunique())
            print('The number of unique license plates in the grouped dataframe is: ', dfFinalGrouped['license_plate'].nunique())
            print('########################################################################################')
            # dataframe, dfSensitivity, dfCount = stmod.chunkedAggregator(dataframe, configDict)
            #                 
        elif dataTapChoice == 2 or 3:

            #aggregating dataframe for data tapping options 2 & 3
            # print('########################################################################################')
            print('The chunk number is: ', (len(lengthList)))
            if len(lengthList) == 1:
                dfGrouped = dataframe.groupby(['HAT','Date','license_plate']).agg(
                                        count=(groupByCol,'count'),
                                        sum=(groupByCol,'sum'),
                                        max=(groupByCol,'max'),
                                        min=(groupByCol,'min')).reset_index()
                        
                dfFinalGrouped = dfGrouped  
                # print(file)
            elif (len(lengthList) > 1):
                dfGrouped = dataframe.groupby(['HAT','Date','license_plate']).agg(
                                        count=(groupByCol,'count'),
                                        sum=(groupByCol,'sum'),
                                        max=(groupByCol,'max'),
                                        min=(groupByCol,'min')).reset_index()
                        
                dfGrouped = pd.concat([dfGrouped, dfFinalGrouped],  ignore_index=True)
                # print(file)
                # print('dfGrouped')
                # print(dfGrouped)

                dfGroupedCombined = dfGrouped.groupby(['HAT','Date','license_plate']).agg({
                                                            'count': 'sum',
                                                            'sum': 'sum',
                                                            'max':'max',
                                                            'min':'min'}).reset_index()
                dfFinalGrouped = dfGroupedCombined
            dfFinalGrouped['mean'] = np.round((dfFinalGrouped['sum']/dfFinalGrouped['count']), 2)
            # print('')
            # print('dfFinalGrouped before filtering')
            # print(dfFinalGrouped) 

            # print('########################################################################################')
            print('The length of the grouped dataframe is ',len(dfFinalGrouped), 'for chunk ',len(lengthList))
            print('No. of Unique HATs of the grouped dataframe is: ', dfFinalGrouped['HAT'].nunique())
            print('The number of unique license plates in the grouped dataframe is: ', dfFinalGrouped['license_plate'].nunique())
            print('########################################################################################')
            # dataframe, dfSensitivity, dfCount = stmod.chunkedAggregator(dataframe, configDict)


    #calculating timerange of the dataset
    timeRange = stmod.timeRange(dfFinalGrouped) 

    #compute K
    K = stmod.KCompute(dfFinalGrouped) 

    #filtering the aggregated chunks
    dataframe = stmod.filtering(dfFinalGrouped, configDict)

    #sensitivity computation
    dfSensitivity, dfCount = stmod.sensitivityFrame(dataframe) 

    if dataTapChoice == 1:
        #pseudonymizing the true values for the license plates
        preProcessedDataframe = premod.pseudonymize(dataframe, configDict)
        postmod.outputFileSpatioTemporal(dataTapChoice, preProcessedDataframe)
        exit(0)
    else:
        preProcessedDataframe = dataframe
        # print(preProcessedDataframe)

        return preProcessedDataframe, configDict, timeRange, dfSensitivity, dfCount, K

def runSpatioTemporalPipeline(dataframe, configDict, K, timeRange, dfCount):

    dfGrouped = dataframe

    print('\n################################################################\n')
    print('APPLYING DIFFERENTIAL PRIVACY')

    #query building
    dfQuery1 = stmod.ITMSQuery1(dfGrouped)
    dfQuery2 = stmod.ITMSQuery2(dfGrouped, configDict)
    
    #signal assignment
    signalQuery1 = dfQuery1['queryOutput'].reset_index(drop = True)
    signalQuery2 = dfQuery2['queryOutput'].reset_index(drop = True)

    #compute sensitivity

    print('\n################################################################\n')
    print('COMPUTING SENSITIVITY')

    sensitivityITMSQuery1, sensitivityITMSQuery2 = stmod.sensitivityComputeITMSQuery(configDict, timeRange, dfCount)
    
    #compute noise 
    print('\n################################################################\n')
    print('COMPUTING NOISE')

    dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
    return dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2

def postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict, dataTapChoice):
    print('\n################################################################\n')
    print('POSTPROCESSING')

    #postprocessing
    dfNoiseQuery1.name = 'dfFinalQuery1'
    dfNoiseQuery2.name = 'dfFinalQuery2'
    dfFinalQuery1 = postmod.postProcessingSpatioTemporal(dfNoiseQuery1, configDict)
    dfFinalQuery2 = postmod.postProcessingSpatioTemporal(dfNoiseQuery2, configDict)
    
    #noise assignment
    noiseQuery1 = dfNoiseQuery1['queryNoisyOutput'].reset_index(drop = True)
    noiseQuery2 = dfNoiseQuery2['queryNoisyOutput'].reset_index(drop = True)
    
    #signal to noise computation
    if dataTapChoice==3:
        snr1 = stmod.snrCompute(signalQuery1, bVarianceQuery1)
        snr2 = stmod.snrCompute(signalQuery2, bVarianceQuery2)
        postmod.RMSEGraph(snr1,configDict["privacyLossBudgetEpsQuery"][0],'spatioQuery1.png')
        print("Relative RMSE Graph generated for query1 check the pipelineOuput Folder")
        postmod.RMSEGraph(snr2,configDict["privacyLossBudgetEpsQuery"][1],'spatioQuery2.png')
        print("Relative RMSE Graph generated for query2 check the pipelineOuput Folder")
        # print('\n\nFor Query 1: ')
    # postmod.signalToNoise(snrAverageQuery1, configDict)
    # maeQuery1 = stmod.maeCompute(signalQuery1, noiseQuery1)
    # print("The MAE is: ", maeQuery1)
    # print('\n\nFor Query 2: ')
    # postmod.signalToNoise(snrAverageQuery2, configDict)
    # maeQuery2 = stmod.maeCompute(signalQuery2, noiseQuery2)
    # print("The MAE is: ", maeQuery2)
    
    #computing and displaying cumulative epsilon
    # postmod.cumulativeEpsilon(configDict)
    
    #creating the output files
    postmod.outputFileSpatioTemporal(dataTapChoice, dfFinalQuery1, dfFinalQuery2)
    return

def main():
    with open("../config/spatioTemporalFNList.json", "r") as config_file:
        file_names_list = json.load(config_file)

    #running predefined functions
    configFileName = '../config/SESpatioTemporalConfig.json'
    with open(configFileName, "r") as cfile:
        configDict = json.load(cfile)
    schemaFileName = 'SESpatioTemporalSchema.json'
    premod.schemaValidator(schemaFileName, configFileName)


    # handling the choice for data tapping, including validation of choice
    validChoice = 0
    while validChoice == 0: 
        print("Select type of output file: ")
        print('''1. Pseudonymized and Aggregated Output (Non-DP) \n2. Clean Query Output \n3. Noisy Query Output ''')
        dataTapChoice = int(input("Enter a number to make your selection: "))
        if dataTapChoice == 1:
            configDict['outputOptions'] = 1
            validChoice = 1
        elif dataTapChoice == 2:
            configDict['outputOptions'] = 2
            validChoice = 1
        elif dataTapChoice == 3:
            configDict['outputOptions'] = 3
            validChoice = 1
        else:
            print("Choice Invalid, please enter an integer between 1 and 3 to make your choice. \\ ")

    preProcessedDataframe, configDict, timeRange, dfSensitivity, dfCount, K = chunkHandling(configFileName, schemaFileName, file_names_list, dataTapChoice)

    dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2 = runSpatioTemporalPipeline(preProcessedDataframe, configDict, K, timeRange, dfCount)

    postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict, dataTapChoice)

if __name__ == '__main__':
    main()
