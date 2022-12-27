import pandas as pd
import modules

#validating the config file against the schema
modules.schemaValidator('DPSchema.json', 'DPConfig.json')

#reading the file and dropping any duplicates
df, configDict, genType = modules.readFile('DPConfig.json')

#supressing any columns that may not be required in the final output
df = modules.suppress(df, configDict)

#generalization applied to categories like time, location, etc that may contain personal identifiable information
df = modules.categorize(df, configDict, genType)

#computing the first query for ITMS
dfAggregateQuery1, K = modules.aggregateStats1(df, configDict)

#computing the second query for ITMS (average number of instances a bus exceeds a speed threshold.)
dfAggregateQuery2, sensitivity2 = modules.aggregateStats2(df, configDict)

#adding noise to the output of aggregateStats1
noisySpeed = modules.variableNoiseAddition1(dfAggregateQuery1, configDict, K)


