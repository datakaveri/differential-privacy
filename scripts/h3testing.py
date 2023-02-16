import modules as mod

#validating the config file against the schema
mod.schemaValidator('DPSchema.json', 'DPConfig.json')

#reading the file and dropping any duplicates
df, configDict, genType = mod.readFile('DPConfig.json')

#supressing any columns that may not be required in the final output
df = mod.suppress(df, configDict)

#generalization applied to categories like time, location, etc that may contain personal identifiable information
df = mod.categorize(df, configDict, genType)
 
#new lines
dfAggregated = mod.aggregator(df, configDict)
mod.sensitivityCompute(df, configDict)

# #computing the first query for ITMS
# dfAggregateQuery1, K, lowerClip1, upperClip1 = mod.aggregateStats1(df, configDict)

# #computing the second query for ITMS (average number of instances a bus exceeds a speed threshold.)
# dfAggregateQuery2, timeRange = mod.aggregateStats2(df, configDict)

# #adding noise to the output of aggregateStats1
# dfNoisyQuery1, b1 = mod.variableNoiseAddition1(dfAggregateQuery1, configDict, K)

# #adding noise to the output of aggregateStats2

# dfNoisyQuery2, b2 = mod.variableNoiseAddition2(dfAggregateQuery2, configDict, K)

# #calculating signal to noise ratio
# trueValue = configDict['trueValue']
# snr1 = mod.signalToNoise(dfNoisyQuery1[trueValue], b1)
# snr2 = mod.signalToNoise(dfNoisyQuery2['aggregateValue'], b2)

# #postprocessing (clipping and rounding and dropping unnecessary information)
# dfFinal1 = mod.postProcessing(dfNoisyQuery1, configDict, lowerClip1, upperClip1)
# dfFinal2 = mod.postProcessing(dfNoisyQuery2, configDict)

# dfFinal1.to_csv('dfFinal1.csv')
# dfFinal2.to_csv('dfFinal2.csv')
