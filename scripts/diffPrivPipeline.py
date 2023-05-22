import spatioTemporalModules as stmod
import categoricalModules as cmod
import preProcessing as premod
import postProcessing as postmod


def preProcessing():
    print('\n####################################################################\n')
    print('PREPROCESSING')

    #validating the config file against the schema
    premod.schemaValidator('DPSchema.json', 'DPConfig.json')

    #reading the file and dropping any duplicates
    df, configDict, genType = premod.readFile('DPConfigITMS.json')

    #dropping duplicates
    df = premod.dropDuplicates(df, configDict)

    #supressing any columns that may not be required in the final output
    preProcessedDataframe = premod.suppress(df, configDict)

    return preProcessedDataframe, configDict, genType

def runSpatioTemporalPipeline(dataframe, configDict):

    #generalization applied to categories like time, location, etc that may contain personal identifiable information
    df = stmod.spatioTemporalGeneralization(dataframe, configDict)

    #calculating timerange of the dataset
    timeRange = stmod.timeRange(df) 
    
    #compute N
    N, dfN = stmod.NCompute(df)
    
    #compute K
    K = stmod.KCompute(df) 
    
    #aggregating dataframe
    dfGrouped, dfSensitivity, dfCount = stmod.aggregator(df, configDict)

    print('\n################################################################\n')
    print('APPLYING DIFFERENTIAL PRIVACY')

    #query building

    # choosing appropriate query 1 from config file "optimized" value
    if configDict["optimized"] == True:
        dfQuery1, dfNoiseQuery1a = stmod.ITMSQuery1a(dfGrouped, K, configDict)
    else:
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


    # //TODO: better implementation of assigning NoiseQuery1a 
    if configDict["optimized"] == False:
        dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
    else:
        dfNoiseQuery1, dfNoiseQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
        dfNoiseQuery1 = dfNoiseQuery1a

    # print("Query1 columns", dfQuery1.columns)
    # print("noise df", dfNoiseQuery1.columns)
    # print(dfNoiseQuery1["queryOutput"])
    # print(signalQuery1)
    return dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2

def runHistoPipeline(dataframe):

    #------------------QUERY 1---------------------------------------------------
    #query building
    histQuery1 = cmod.histogramQuery1(dataframe, configDict)
    
    #compute noiseS
    noiseHistQuery1, bVarianceQuery1 = cmod.noiseComputeHistogramQuery1(histQuery1, configDict)   
    
    #postprocessing
    dfFinalQuery1 = cmod.postProcessingQuery(noiseHistQuery1, configDict, genType)
           
    #histogram and csv generation
    cmod.histogramAndOutputQuery(dfFinalQuery1, configDict, genType, query = 1)
    
    #signal to noise computation
    cmod.snrQuery(noiseHistQuery1, bVarianceQuery1, configDict)       
        
    #------------------QUERY 2---------------------------------------------------
    
    histQuery2 = cmod.histogramQuery2(dataframe, configDict)
    
    noiseHistQuery2, bVarianceQuery2 = cmod.noiseComputeHistogramQuery2(histQuery2, configDict)
    
    #postprocessing
    dfFinalQuery2 = cmod.postProcessingQuery(noiseHistQuery2, configDict, genType)
    
    cmod.snrQuery(noiseHistQuery2, bVarianceQuery2, configDict)     
    
    #histogram and csv generation
    cmod.histogramAndOutputQuery(dfFinalQuery2, configDict, genType, query = 2)

    modeHistQuery2Alt, dfFinalHistQuery2Alt = cmod.exponentialMechanismHistogramQuery2(histQuery2, configDict)

    postmod.outputFile(dfFinalHistQuery2Alt, 'dfNoisySoil2')        
   
    return

def postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict, genType):
    print('\n################################################################\n')
    print('POSTPROCESSING')
    #postprocessing
    dfFinalQuery1 = postmod.postProcessing(dfNoiseQuery1, configDict, genType)
    dfFinalQuery2 = postmod.postProcessing(dfNoiseQuery2, configDict, genType)
    
    #noise assignment
    noiseQuery1 = dfNoiseQuery1['queryNoisyOutput'].reset_index(drop = True)
    noiseQuery2 = dfNoiseQuery2['queryNoisyOutput'].reset_index(drop = True)
    
    #signal to noise computation
    snrAverageQuery1 = stmod.snrCompute(signalQuery1, bVarianceQuery1)
    snrAverageQuery2 = stmod.snrCompute(signalQuery2, bVarianceQuery2)
    postmod.signalToNoise(snrAverageQuery1, configDict)
    postmod.signalToNoise(snrAverageQuery2, configDict)
    
    #computing and displaying cumulative epsilon
    postmod.cumulativeEpsilon(configDict)
    
    #creating the output files
    postmod.outputFile(dfFinalQuery1, 'dfNoisyITMS1')
    postmod.outputFile(dfFinalQuery2, 'dfNoisyITMS2')
    
    print('Differentially Private output generated. Please check the pipelineOutput folder.')
    print('\n################################################################\n')
    return

#running predefined functions
preProcessedDataframe, configDict, genType = preProcessing()

#choosing pipeline based on dataset type
if genType == "spatio-temporal":
    # dataframe = spatioTemporalGeneralization(dataframe, configFile)
    dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2 = runSpatioTemporalPipeline(preProcessedDataframe, configDict)
elif genType == "categorical":
    dataframe = cmod.categoricGeneralization(preProcessedDataframe, configDict)
    runHistoPipeline(dataframe)

#running postprocessing functions
postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict, genType)