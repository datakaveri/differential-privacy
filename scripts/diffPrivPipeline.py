import spatioTemporalModules as stmod
import categoricalModules as cmod
import genAgg as genAgg
import preProcessing as premod
import postProcessing as postmod


def preProcessing():
    #validating the config file against the schema
    print('\n####################################################################\n')
    print('\nSelect the desired configuration file: ')
    print('\n1. SpatioTemporal Config')
    print('2. Categorical Config')
    print('3. genAgg Config')
    print('\n####################################################################\n')

    configNum = int(input('Enter a number: '))

    if configNum == 1:
        configFileName = 'DPConfigSpatioTemporal.json'
        schemaFileName = 'DPSchemaSpatioTemporal.json'
    elif configNum == 2:
        configFileName = 'DPConfigCategorical.json'
        schemaFileName = 'DPSchemaCategorical.json'
    elif configNum == 3:
        configFileName = 'DPConfigGenAgg.json'
        schemaFileName = 'DPSchemaGenAgg.json'
    
    print('\n####################################################################\n')
    print('PREPROCESSING')
    # premod.schemaValidator(schemaFileName, configFileName)

    #reading the file and dropping any duplicates
    df, configDict, genType = premod.readFile(configFileName)


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
        dfQuery1, dfNoiseQuery1a, bVarianceQuery1a = stmod.ITMSQuery1a(dfGrouped, K, configDict)
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


    if configDict["optimized"] == False:
        dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
    else:
        dfNoiseQuery1, dfNoiseQuery2, bVarianceQ3uery1, bVarianceQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
        dfNoiseQuery1 = dfNoiseQuery1a
        bVarianceQuery1 = bVarianceQuery1a

    return dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2

def runCategoricalPipeline(df, configDict):

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

def runGenAggpipeline(df, configDict):
    WAYMDCounts_dict= genAgg.Generalization(df, configDict)
    return WAYMDCounts_dict

def postProcessingCategorical(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, noiseHistQuery1, noiseHistQuery2, configDict, genType):
    print('\n################################################################\n')
    print('POSTPROCESSING')
    #Query 1
    #postprocessing
    dfFinalQuery1 = cmod.postProcessingQuery(noiseHistQuery1, configDict, genType)
    
    #signal to noise computation
    print('\n\nSNR for Query 1: ')
    cmod.snrQuery(noiseHistQuery1, bVarianceQuery1, configDict)       

    #histogram and csv generation
    dfFinalQuery1 = cmod.histogramAndOutputQuery(dfFinalQuery1, configDict, genType, query = 1)
    
    

    #----------------------QUERY 2------------------------------------------------
    
    #Query 2
    #postprocessing
    dfFinalQuery2 = cmod.postProcessingQuery(noiseHistQuery2, configDict, genType)
    
    #signal to noise computation
    print('\n\nSNR for Query 2: ')
    cmod.snrQuery(noiseHistQuery2, bVarianceQuery2, configDict)     
    
    #histogram and csv generation
    dfFinalQuery2 = cmod.histogramAndOutputQuery(dfFinalQuery2, configDict, genType, query = 2)
    
    
    #final output file generation
    cmod.mergeDicts(dfFinalQuery1, dfFinalQuery2)
    print('\nDifferentially Private output generated. Please check the pipelineOutput folder.')
    return dfFinalQuery1, dfFinalQuery2

def postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict, genType):
    print('\n################################################################\n')
    print('POSTPROCESSING')
    #postprocessing
    dfNoiseQuery1.name = 'dfFinalQuery1'
    dfNoiseQuery2.name = 'dfFinalQuery2'
    dfFinalQuery1 = postmod.postProcessing(dfNoiseQuery1, configDict, genType)
    dfFinalQuery2 = postmod.postProcessing(dfNoiseQuery2, configDict, genType)
    
    #noise assignment
    noiseQuery1 = dfNoiseQuery1['queryNoisyOutput'].reset_index(drop = True)
    noiseQuery2 = dfNoiseQuery2['queryNoisyOutput'].reset_index(drop = True)
    
    #signal to noise computation
    if configDict["optimized"] == False:
        snrAverageQuery1 = stmod.snrCompute(signalQuery1, bVarianceQuery1)
        snrAverageQuery2 = stmod.snrCompute(signalQuery2, bVarianceQuery2)
        print('\n\nFor Query 1: ')
        postmod.signalToNoise(snrAverageQuery1, configDict)
        maeQuery1 = stmod.maeCompute(signalQuery1, noiseQuery1)
        print("The MAE is: ", maeQuery1)
        print('\n\nFor Query 2: ')
        postmod.signalToNoise(snrAverageQuery2, configDict)

        maeQuery2 = stmod.maeCompute(signalQuery2, noiseQuery2)
        print("The MAE is: ", maeQuery2)

    else:
        maeQuery1 = stmod.maeCompute(signalQuery1, noiseQuery1)
        maeQuery2 = stmod.maeCompute(signalQuery2, noiseQuery2)
        snrAverageQuery2 = stmod.snrCompute(signalQuery2, bVarianceQuery2)
        postmod.signalToNoise(snrAverageQuery2, configDict)
    
    #computing and displaying cumulative epsilon
    postmod.cumulativeEpsilon(configDict)
    
    #creating the output files
    postmod.outputFileSpatioTemporal(dfFinalQuery1, dfFinalQuery2)
    
    print('Differentially Private output generated. Please check the pipelineOutput folder.')
    print('\n################################################################\n')
    return
def postProcessingGenAgg(dict):
    postmod.outputFileGenAgg(dict)
    print('Generalized Aggregated output generated. Please check the pipelineOutput folder.')
    print('\n################################################################\n')
#running predefined functions
preProcessedDataframe, configDict, genType = preProcessing()

#choosing pipeline based on dataset type
if genType == "spatio-temporal":
    # dataframe = spatioTemporalGeneralization(dataframe, configFile)
    dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2 = runSpatioTemporalPipeline(preProcessedDataframe, configDict)
    postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, bVarianceQuery1, bVarianceQuery2, signalQuery1, signalQuery2, configDict, genType)
elif genType == "categorical":
    histQuery1, histQuery2, bVarianceQuery1, bVarianceQuery2, noiseHistQuery1, noiseHistQuery2 = runCategoricalPipeline(preProcessedDataframe, configDict)
    dfFinalQuery1, dfFinalQuery2 = postProcessingCategorical(histQuery1, histQuery2, bVarianceQuery1, bVarianceQuery2, noiseHistQuery1, noiseHistQuery2, configDict, genType)
elif genType == "genAgg":
    WAYMDCounts_dict=runGenAggpipeline(preProcessedDataframe, configDict)
    postProcessingGenAgg(WAYMDCounts_dict)

