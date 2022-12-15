import pandas as pd
import modules
import generalization

df, configDict, genType, b = modules.readFile('DPConfig.json')

df = generalization.categorize(df, configDict, genType)

dfAggregateQuery1, sensitivity = modules.aggregateStats1(df)

dfAggregateQuery2, sensitivity = modules.aggregateStats2(df, configDict['speed_limit'])
#df, noise, ep = modules.noiseAddition(df, localSensitivity, b)



#print(df['h3index'])
#print('8742d98b6ffffff' in df['h3index'].unique())
#dlist = modules.avg_query_noise_q1(df)


#print(avgspeed, localSensitivity)
#print(noise, ep)

