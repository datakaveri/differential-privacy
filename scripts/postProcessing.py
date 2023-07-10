import numpy as np
import pandas as pd
import json 

def postProcessing(dfNoise, configDict, genType):
    if genType == 'spatio-temporal':
        #postprocessing ITMSQuery1
        globalMaxValue = configDict['globalMaxValue']
        globalMinValue = configDict['globalMinValue']

        if dfNoise.name == 'dfFinalQuery1':
            dfFinalITMSQuery1 = dfNoise
            dfFinalITMSQuery1['queryNoisyOutput'].clip(globalMinValue, globalMaxValue, inplace = True)
            if configDict['optimized'] == False:
                dfFinalITMSQuery1.drop(['queryOutput'], axis = 1, inplace = True)
            return dfFinalITMSQuery1
        elif dfNoise.name == 'dfFinalQuery2':
            dfFinalITMSQuery2 = dfNoise
            dfFinalITMSQuery2['queryNoisyOutput'].clip(0, np.inf, inplace = True)
            if configDict['optimized'] == False:
                dfFinalITMSQuery2.drop(['queryOutput'], axis = 1, inplace = True)
            return dfFinalITMSQuery2

    elif genType == 'categorical':
        dfFinal = dfNoise
        dfFinal['roundedNoisyCount'] = dfFinal['noisyCount'].round()
        dfFinal['roundedNoisyCount'].clip(0, np.inf, inplace = True)
        dfFinal.drop(['noisyCount'], axis = 1, inplace = True)
        return dfFinal

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

def outputFileSpatioTemporal(dfFinalQuery1, dfFinalQuery2):
    dfFinal = pd.DataFrame()
    dfFinal['HAT'] = dfFinalQuery1['HAT']
    dfFinal['query1NoisyOutput'] = dfFinalQuery1['queryNoisyOutput'].round(3)
    dfFinal['query2NoisyOutput'] = dfFinalQuery2['queryNoisyOutput'].round(3)
    dfFinal = createNestedJSON(dfFinal, 'HAT')
    outputFile = '../pipelineOutput/noisyOutputSpatioTemporal.json'
    with open(outputFile, 'w') as file:
        json.dump(dfFinal, file, indent=4)
    return
