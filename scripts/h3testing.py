import pandas as pd
import modules
import generalization

df, configDict, genType = modules.readFile('DPConfig.json')


df2 = generalization.categorize(df, configDict, genType)

print(df2)
