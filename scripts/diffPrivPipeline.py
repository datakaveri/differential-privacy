import spatioTemporalModules as stmod
import categoricalModules as cmod
import genAgg as genAgg
import preProcessing as premod
import postProcessing as postmod
import json
import pandas as pd
import numpy as np

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
    
    # #validating the config file against the schema
    # print('\n####################################################################\n')
    # print('\nSelect the desired configuration file: ')
    # print('\n1. SpatioTemporal Config')
    # # print('2. Categorical Config')
    # # print('\n####################################################################\n')

    # configNum = int(input('Enter a number: '))
    # if configNum == 1:
    configFileName = '../config/DPSEConfigST.json'
    with open(configFileName, "r") as cfile:
        configDict = json.load(cfile)
    # schemaFileName = 'DPSchemaSpatioTemporal.json'
    # elif configNum == 2:
    #     configFileName = 'DPConfigCategorical.json'
    #     schemaFileName = 'DPSchemaCategorical.json'
    
    print('\n####################################################################\n')
    print('PREPROCESSING')
    lengthList = []
    dfFinalGrouped = pd.DataFrame()

    groupByCol = configDict['groupByCol']
    
    for file in file_names_list:


        lengthList.append(file)
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframe = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframe.shape))
            configDict['dataFile'] == file

        # premod.schemaValidator(schemaFileName, configFileName)

        #reading the file and dropping any duplicates
        # dataframe, configDict = premod.readFile(configFileName)

        #dropping duplicates
        dataframe = premod.dropDuplicates(dataframe, configDict)

        #supressing any columns that may not be required in the final output
        dataframe = premod.suppress(dataframe, configDict)

        #generalization applied to categories like time, location, etc that may contain personal identifiable information
        dataframe = stmod.spatioTemporalGeneralization(dataframe, configDict)
        
        #aggregating dataframe
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
        # dataframe, dfSensitivity, dfCount = stmod.chunkedAggregator(dataframe, configDict)

    #calculating timerange of the dataset
    timeRange = stmod.timeRange(dfFinalGrouped) 

    #compute K
    K = stmod.KCompute(dfFinalGrouped) 

    #filtering the aggregated chunks
    dataframe = stmod.filtering(dfFinalGrouped, configDict)

    #sensitivity computation
    dfSensitivity, dfCount = stmod.sensitivityFrame(dataframe) 

    preProcessedDataframe = dataframe
    return preProcessedDataframe, configDict, timeRange, dfSensitivity, dfCount, K

def runSpatioTemporalPipeline(dataframe, configDict, K):

    dfGrouped = dataframe

    print('\n################################################################\n')
    print('APPLYING DIFFERENTIAL PRIVACY')

    #query building

    # choosing appropriate query 1 from config file "optimized" value
    # if configDict["optimized"] == True:
    #     dfQuery1, dfNoiseQuery1a, bVarianceQuery1a = stmod.ITMSQuery1a(dfGrouped, K, configDict)
    # else:
    dfQuery1 = stmod.ITMSQuery1(dfGrouped)
    dfQuery2 = stmod.ITMSQuery2(dfGrouped, configDict)
    # dfQuery1Weighted = stmod.ITMSQuery1Weighted(dfGrouped)
    
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


    # if configDict["optimized"] == False:
    dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
    # else:
    #     dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
    #     dfNoiseQuery1 = dfNoiseQuery1a
    #     bVarianceQuery1 = bVarianceQuery1a

    return dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2

# def runCategoricalPipeline(df, configDict):

    dataframe = cmod.categoricGeneralization(df, configDict)

    #------------------QUERY 1---------------------------------------------------
    #query building
    histQuery1 = cmod.histogramQuery1(dataframe, configDict)
    
    #compute noise
    noiseHistQuery1, bVarianceQuery1 = cmod.noiseComputeHistogramQuery1(histQuery1, configDict)       
        
    #------------------QUERY 2---------------------------------------------------
    
    #query building
    histQuery2, allCols = cmod.histogramQuery2(dataframe, configDict)
    
    #compute noise
    noiseHistQuery2, bVarianceQuery2 = cmod.noiseComputeHistogramQuery2(histQuery2, allCols, configDict)    

    #modeHistQuery2Alt, dfFinalHistQuery2Alt = cmod.exponentialMechanismHistogramQuery2(histQuery2, configDict)

    #postmod.outputFile(dfFinalHistQuery2Alt, 'dfNoisySoil2')        
   
    return histQuery1, histQuery2, bVarianceQuery1, bVarianceQuery2, noiseHistQuery1, noiseHistQuery2

# def postProcessingCategorical(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, noiseHistQuery1, noiseHistQuery2, configDict, genType):
    print('\n################################################################\n')
    print('POSTPROCESSING')
    #Query 1
    #postprocessing
    dfFinalQuery1 = cmod.postProcessingQuery(noiseHistQuery1, configDict, genType)
    
    #signal to noise computation
    print('\n\nSNR for Query 1: ')
    snr1=cmod.snrQuery(noiseHistQuery1, bVarianceQuery1, configDict)       

    #postmod.RMSEGraph(snr1,configDict["PrivacyLossBudget"][0],'categoricalQuery1.png')

    #histogram and csv generation
    dfFinalQuery1 = cmod.histogramAndOutputQuery(dfFinalQuery1, configDict, genType, query = 1)
    
    

    #----------------------QUERY 2------------------------------------------------
    
    #Query 2
    #postprocessing
    dfFinalQuery2 = cmod.postProcessingQuery(noiseHistQuery2, configDict, genType)
    
    #signal to noise computation
    print('\n\nSNR for Query 2: ')
    snr2=cmod.snrQuery(noiseHistQuery2, bVarianceQuery2, configDict)     
    #postmod.RMSEGraph(snr2,configDict["PrivacyLossBudget"][1],'categoricalQuery2.png')

    #histogram and csv generation
    dfFinalQuery2 = cmod.histogramAndOutputQuery(dfFinalQuery2, configDict, genType, query = 2)
    
    
    #final output file generation
    postmod.outputFileCategorical(dfFinalQuery1, dfFinalQuery2)
    print('\nDifferentially Private output generated. Please check the pipelineOutput folder.')
    return dfFinalQuery1, dfFinalQuery2

def postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict):
    print('\n################################################################\n')
    print('POSTPROCESSING')
    #postprocessing
    dfNoiseQuery1.name = 'dfFinalQuery1'
    dfNoiseQuery2.name = 'dfFinalQuery2'
    dfFinalQuery1 = postmod.postProcessing(dfNoiseQuery1, configDict)
    dfFinalQuery2 = postmod.postProcessing(dfNoiseQuery2, configDict)
    
    #noise assignment
    noiseQuery1 = dfNoiseQuery1['queryNoisyOutput'].reset_index(drop = True)
    noiseQuery2 = dfNoiseQuery2['queryNoisyOutput'].reset_index(drop = True)
    
    #signal to noise computation
    if configDict["optimized"] == False:
        snrAverageQuery1,snr1 = stmod.snrCompute(signalQuery1, bVarianceQuery1)
        snrAverageQuery2,snr2 = stmod.snrCompute(signalQuery2, bVarianceQuery2)
        print('\n\nFor Query 1: ')
        postmod.RMSEGraph(snr1,configDict["privacyLossBudgetEpsQuery"][0],'spatioQuery1.png')
        postmod.signalToNoise(snrAverageQuery1, configDict)
        maeQuery1 = stmod.maeCompute(signalQuery1, noiseQuery1)
        print("The MAE is: ", maeQuery1)
        print('\n\nFor Query 2: ')
        postmod.RMSEGraph(snr2,configDict["privacyLossBudgetEpsQuery"][1],'spatioQuery2.png')
        postmod.signalToNoise(snrAverageQuery2, configDict)

        maeQuery2 = stmod.maeCompute(signalQuery2, noiseQuery2)
        print("The MAE is: ", maeQuery2)

    # else:
    #     maeQuery1 = stmod.maeCompute(signalQuery1, noiseQuery1)
    #     maeQuery2 = stmod.maeCompute(signalQuery2, noiseQuery2)
    #     snrAverageQuery2 = stmod.snrCompute(signalQuery2, bVarianceQuery2)
    #     postmod.signalToNoise(snrAverageQuery2, configDict)
    
    #computing and displaying cumulative epsilon
    postmod.cumulativeEpsilon(configDict)
    
    postmod.outputFile(dfFinalQuery1, dfFinalQuery2)
    
    print('Differentially Private output generated. Please check the pipelineOutput folder.')
    print('\n################################################################\n')
    return
def postProcessingGenAgg(dict):
    postmod.outputFileGenAgg(dict)
    print('Generalized Aggregated output generated. Please check the pipelineOutput folder.')
    print('\n################################################################\n')
    
#running predefined functions
preProcessedDataframe, configDict, timeRange, dfSensitivity, dfCount, K = chunkHandling()
dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2 = runSpatioTemporalPipeline(preProcessedDataframe, configDict, K)
postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict)


