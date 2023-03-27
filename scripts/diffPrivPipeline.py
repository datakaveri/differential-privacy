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

dfQuery3 = mod.histogramQuery1(df, configDict)

#ref_df = groupedDF
#df.to_csv('CombinedThreeDistrictsCLEANED2.csv', index=False)

dfNoiseQuery3, Noise = mod.noiseComputeHistogramQuery(dfQuery3.copy(), configDict)

#signal assignment
signalQuery3 = dfQuery3['Count'].reset_index(drop = True)

#postprocessing
FinalDF = mod.postProcessing(dfNoiseQuery3.copy(), configDict, genType)

#noise assignment
noiseQuery3 = dfNoiseQuery3['noisyCount'].reset_index(drop = True)

#signal to noise computation
mod.signalToNoise(signalQuery3, noiseQuery3, configDict)

mod.printHistogram(FinalDF)



#calculating timerange of the dataset
timeRange = mod.timeRange(df) 

#compute N
N = mod.NCompute(df)

#compute K
K = mod.KCompute(df) 

#aggregating dataframe
dfGrouped = mod.aggregator(df, configDict)
print('\n################################################################\n')

print('\n################################################################\n')
print('APPLYING DIFFERENTIAL PRIVACY')

#query building
dfQuery1 = mod.ITMSQuery1(dfGrouped)
dfQuery2 = mod.ITMSQuery2(dfGrouped, configDict)


#signal assignment
signalQuery1 = dfQuery1['query1Output'].reset_index(drop = True)
signalQuery2 = dfQuery2['query2Output'].reset_index(drop = True)

#compute sensitivity
sensitivityITMSQuery1, sensitivityITMSQuery2 = mod.ITMSSensitivityCompute(configDict, timeRange, N)

#compute noise 
dfNoiseQuery1, dfNoiseQuery2 = mod.noiseComputeITMSQuery(dfQuery1, dfQuery2, sensitivityITMSQuery1, sensitivityITMSQuery2, configDict, K)

#postprocessing
dfFinalQuery1, dfFinalQuery2 = mod.postProcessing(dfNoiseQuery1, dfNoiseQuery2, configDict)

#noise assignment
noiseQuery1 = dfNoiseQuery1['query1NoisyOutput'].reset_index(drop = True)
noiseQuery2 = dfNoiseQuery2['query2NoisyOutput'].reset_index(drop = True)

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


