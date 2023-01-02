import pandas as pd
import modules as mod

#validating the config file against the schema
mod.schemaValidator('DPSchema.json', 'DPConfig.json')

#reading the file and dropping any duplicates
df, configDict, genType = mod.readFile('DPConfig.json')

#supressing any columns that may not be required in the final output
df = mod.suppress(df, configDict)

#generalization applied to categories like time, location, etc that may contain personal identifiable information
df = mod.categorize(df, configDict, genType)

#computing the first query for ITMS
dfAggregateQuery1, K = mod.aggregateStats1(df, configDict)

#computing the second query for ITMS (average number of instances a bus exceeds a speed threshold.)
dfAggregateQuery2, timeRange = mod.aggregateStats2(df, configDict)

#adding noise to the output of aggregateStats1
dfNoisyQuery1 = mod.variableNoiseAddition1(dfAggregateQuery1, configDict, K)

#adding noise to the output of aggregateStats2
dfNoisyQuery2 = mod.variableNoiseAddition2(dfAggregateQuery2, configDict, timeRange, K)