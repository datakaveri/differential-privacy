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
    df, configDict, genType = premod.readFile('DPConfig.json')

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
    dfNoiseQuery1, dfNoiseQuery2 = stmod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)
    
    return dfNoiseQuery1, dfNoiseQuery2, signalQuery1, signalQuery2

def runHistoPipeline(dataframe):
    #query building
    histQuery1 = cmod.histogramQuery1(dataframe, configDict)
    
    #compute noiseS
    noiseHistQuery1 = cmod.noiseComputeHistogramQuery(histQuery1, configDict)   
    
    #postprocessing and histogram display
    for name, dfsNoise in noiseHistQuery1.items():
        dfFinalHistQuery1 = cmod.postProcessing(dfsNoise.copy(), configDict, genType)        
        cmod.printHistogram(dfFinalHistQuery1, name)
        dfFinalHistQuery1 = dfFinalHistQuery1.drop(['Count','Noise'], axis = 1).reset_index(drop = True)
        cmod.outputFile(dfFinalHistQuery1, 'dfNoisySoil1_' + name)
        
    #signal to noise computation
    for name, finalDF in noiseHistQuery1.items():
        #signal assignment
        signalQuery3 = finalDF['Count'].reset_index(drop = True)
        
        #noise assignment
        noiseQuery3 = finalDF['Noise'].reset_index(drop = True)
        
        #signal to noise computation
        cmod.signalToNoise(signalQuery3, noiseQuery3, configDict)    
        
    histQuery2, histQuery2TrueModeDf = cmod.histogramQuery2(dataframe, configDict)

    noiseHistQuery2, dfFinalHistQuery2 = cmod.noiseComputeHistogramQuery2(histQuery2, configDict)
    cmod.outputFile(dfFinalHistQuery2, 'dfNoisySoil2')    
    
    noiseHistQuery2Alt, histQuery2FinalDFAlt = cmod.reportNoisyMax(histQuery2, configDict)
    
    #cmod.test(noiseHistQuery2, histQuery2, configDict)
    return

def postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, signalQuery1, signalQuery2, configDict, genType):
    print('\n################################################################\n')
    print('POSTPROCESSING')
    #postprocessing
    dfFinalQuery1 = postmod.postProcessing(dfNoiseQuery1, configDict, genType)
    dfFinalQuery2 = postmod.postProcessing(dfNoiseQuery2, configDict, genType)
    
    #noise assignment
    noiseQuery1 = dfNoiseQuery1['queryNoisyOutput'].reset_index(drop = True)
    noiseQuery2 = dfNoiseQuery2['queryNoisyOutput'].reset_index(drop = True)
    
    #signal to noise computation
    postmod.signalToNoise(signalQuery1, noiseQuery1, configDict)
    postmod.signalToNoise(signalQuery2, noiseQuery2, configDict)
    
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
    dfNoiseQuery1, dfNoiseQuery2, signalQuery1, signalQuery2 = runSpatioTemporalPipeline(preProcessedDataframe, configDict)
elif genType == "categorical":
    # dataframe = categoricGeneralization(dataframe, configFile)
    runHistoPipeline()

#running postprocessing functions
postProcessingSpatioTemporal(dfNoiseQuery1, dfNoiseQuery2, signalQuery1, signalQuery2, configDict, genType)