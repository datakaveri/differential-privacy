import modules as mod

print('\n################################################################\n')
print('PREPROCESSING')

#validating the config file against the schema
mod.schemaValidator('DPSchema.json', 'DPConfig.json')

#configJson, configDict, genType = mod.readConfig('DPConfig.json')

#reading the file and dropping any duplicates
df, configDict, genType = mod.readFile('DPConfig.json')

#supressing any columns that may not be required in the final output
df = mod.suppress(df, configDict)

#generalization applied to categories like time, location, etc that may contain personal identifiable information
df = mod.categorize(df, configDict, genType)


def runHistoPipeline():
    #query building
    histQuery1 = mod.histogramQuery1(df, configDict)
    
    #compute noiseS
    noiseHistQuery1 = mod.noiseComputeHistogramQuery(histQuery1, configDict)   
    
    #postprocessing and histogram display
    for name, dfs in noiseHistQuery1.items():
        for pair, dfsNoise in dfs.items():
            dfFinalHistQuery1 = mod.postProcessing(dfsNoise.copy(), configDict, genType)        
            mod.printHistogram(dfFinalHistQuery1, name, pair)
            dfFinalHistQuery1 = dfFinalHistQuery1.drop(['Count','Noise'], axis = 1).reset_index(drop = True)
            mod.outputFile(dfFinalHistQuery1, 'dfNoisySoil1_'+name+'_'+pair)
        
    #signal to noise computation
    for name, finalDF in noiseHistQuery1.items():
        #signal assignment
        signalQuery3 = finalDF['Count'].reset_index(drop = True)
        
        #noise assignment
        noiseQuery3 = finalDF['Noise'].reset_index(drop = True)
        
        #signal to noise computation
        mod.signalToNoise(signalQuery3, noiseQuery3, configDict)    
        
    #histQuery2, histQuery2TrueModeDf = mod.histogramQuery2(df, configDict)
    
    histQuery2Alt = mod.histogramQuery2Alt(df, configDict)

    noiseHistQuery2Alt = mod.noiseComputeHistogramQuery2(histQuery2Alt, configDict)
    
    modeHistQuery2Alt, dfFinalHistQuery2Alt = mod.exponentialMechanismHistogramQuery2(histQuery2Alt, configDict)
    
    mod.outputFile(dfFinalHistQuery2Alt, 'dfNoisySoil2')    
    
    #mod.test(noiseHistQuery2, histQuery2, configDict)

def runSpatioTemporalPipeline():
    #calculating timerange of the dataset
    timeRange = mod.timeRange(df) 
    
    #compute N
    N, dfN = mod.NCompute(df)
    
    #compute K
    K = mod.KCompute(df) 
    
    #aggregating dataframe
    dfGrouped, dfSensitivity, dfCount = mod.aggregator(df, configDict)
    print('\n################################################################\n')
    print('APPLYING DIFFERENTIAL PRIVACY')

    #query building
    dfQuery1 = mod.ITMSQuery1(dfGrouped)
    dfQuery2 = mod.ITMSQuery2(dfGrouped, configDict)
    dfQuery1Weighted = mod.ITMSQuery1Weighted(dfGrouped)
    
    #signal assignment
    signalQuery1 = dfQuery1['queryOutput'].reset_index(drop = True)
    signalQuery2 = dfQuery2['queryOutput'].reset_index(drop = True)
    
    #compute sensitivity
    sensitivityITMSQuery1, sensitivityITMSQuery2, sensitivityITMSQuery1Weighted = mod.ITMSSensitivityCompute(configDict, timeRange, N, dfN, dfSensitivity, dfCount)
    
    #compute noise 
    dfNoiseQuery1, dfNoiseQuery2, dfNoiseQuery1Weighted = mod.noiseComputeITMSQuery(dfQuery1, dfQuery2, dfQuery1Weighted, sensitivityITMSQuery1, sensitivityITMSQuery2, sensitivityITMSQuery1Weighted, configDict, K)
    
    #postprocessing
    dfFinalQuery1 = mod.postProcessing(dfNoiseQuery1, configDict, genType)
    dfFinalQuery2 = mod.postProcessing(dfNoiseQuery2, configDict, genType)
    
    #noise assignment
    noiseQuery1 = dfNoiseQuery1['queryNoisyOutput'].reset_index(drop = True)
    noiseQuery2 = dfNoiseQuery2['queryNoisyOutput'].reset_index(drop = True)
    
    #signal to noise computation
    mod.signalToNoise(signalQuery1, noiseQuery1, configDict)
    mod.signalToNoise(signalQuery2, noiseQuery2, configDict)
    
    #computing and displaying cumulative epsilon
    mod.cumulativeEpsilon(configDict)
    
    #creating the output files
    mod.outputFile(dfFinalQuery1, 'dfNoisyITMS1')
    mod.outputFile(dfFinalQuery2, 'dfNoisyITMS2')
    
    print('Differentially Private output generated. Please check the pipelineOutput folder.')
    print('\n################################################################\n')

if genType == 'categorical':
    runHistoPipeline()
elif genType == 'spatio-temporal':
    runSpatioTemporalPipeline()
