import numpy as np
import pandas as pd
import json

def postProcessing(dfNoise, configDict):
    #postprocessing ITMSQuery1
    globalMaxValue = configDict['globalMaxValue']
    globalMinValue = configDict['globalMinValue']
    dfFinalITMSQuery1 = dfNoise
    dfFinalITMSQuery1['queryNoisyOutput'].clip(globalMinValue, globalMaxValue, inplace = True)
    # if configDict['optimized'] == False:
    #     dfFinalITMSQuery1.drop(['queryOutput'], axis = 1, inplace = True)        
    # #postprocessing ITMS Query 2
    # dfFinalITMSQuery2 = dfNoiseITMSQuery2
    # dfFinalITMSQuery2['query2NoisyOutput'].clip(0, np.inf, inplace = True)
    # dfFinalITMSQuery2.drop(['query2Output'], axis = 1, inplace = True)
    
    return dfFinalITMSQuery1

def signalToNoise(snrAverage,configDict):
    # SNR Threshold
    snrUpperLimit = configDict['snrUpperLimit']
    snrLowerLimit = configDict['snrLowerLimit']

    if snrAverage < snrLowerLimit :
        print("Your Signal to Noise Ratio of " + str(round(snrAverage,3)) + " is below the bound.")
    elif snrAverage > snrUpperLimit:
        print("Your Signal to Noise Ratio of " + str(round(snrAverage,3)) + " is above the bound.")
    else:
        print("Your Signal to Noise Ratio of " + str(round(snrAverage,3)) + " is within the bounds.")
    return snrAverage

def cumulativeEpsilon(configDict):

    privacyLossBudgetQuery1 = configDict['privacyLossBudgetEpsQuery'][0]
    privacyLossBudgetQuery2 = configDict['privacyLossBudgetEpsQuery'][1]
    cumulativeEpsilon = privacyLossBudgetQuery1 + privacyLossBudgetQuery2
    print('\nYour Cumulative Epsilon for the displayed queries is: ' + str(cumulativeEpsilon))
    return cumulativeEpsilon

def createNestedJSON(dataframe, parent_col):
    result = []
    for _, row in dataframe.iterrows():
        current = result
        for col in dataframe.columns:
            if col == parent_col:
                if not any(d.get(parent_col) == row[parent_col] for d in current):
                    current.append({parent_col: row[parent_col]})
                continue
            if pd.notnull(row[col]):
                if col not in current[-1]:
                    current[-1][col] = row[col]
    return result

def outputFile(dfFinalQuery1, dfFinalQuery2):
    dfFinal = pd.DataFrame()
    dfFinal['HAT'] = dfFinalQuery1['HAT']
    dfFinal['query1NoisyOutput'] = dfFinalQuery1['queryNoisyOutput']
    dfFinal['query2NoisyOutput'] = dfFinalQuery2['queryNoisyOutput']
    dfFinal = createNestedJSON(dfFinal, 'HAT')
    outputFile = 'noisyOutput.json'
    with open(outputFile, 'w') as file:
        json.dump(dfFinal, file, indent=4)
    # dfFinal.to_json('../pipelineOutput/' + 'noisyOutput' + '.json')
    return
