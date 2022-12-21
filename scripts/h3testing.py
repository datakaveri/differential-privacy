import pandas as pd
import modules
import generalization

df, configDict, genType, b = modules.readFile('DPConfig.json')

df = modules.suppress(df, configDict)

df = generalization.categorize(df, configDict, genType)

dfAggregateQuery1, K = modules.aggregateStats1(df, configDict)

dfAggregateQuery2, sensitivity2 = modules.aggregateStats2(df, configDict)

#dfNoisy, epsMax, b = modules.noiseAddition1(dfAggregateQuery1, sensitivity1, b, K)

noisySpeed = modules.variableNoiseAddition1(dfAggregateQuery1, configDict['privacy_loss_budget_eps'], K)

#modules.plot(b, epsMax)

#print(df['h3index'])
#print('8742d98b6ffffff' in df['h3index'].unique())
#dlist = modules.avg_query_noise_q1(df)


#print(avgspeed, localSensitivity)
#print(noise, ep)

