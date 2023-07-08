import numpy as np
import pandas as pd

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
        for i, col in enumerate(dataframe.columns):
            if col == parent_col:
                continue
            if i == len(dataframe.columns) - 1:
                current.append(row[col])
            else:
                value = row[col]
                if col not in current[-1]:
                    current[-1][col] = {}
                current = current[-1][col]
                if isinstance(value, pd.DataFrame):
                    current[col] = createNestedJSON(value, parent_col)
                else:
                    current[col] = value
    return result

def outputFile(dfFinalQuery1, dfFinalQuery2):
    dfFinal = pd.DataFrame()
    dfFinal['HAT'] = dfFinalQuery1['HAT']
    dfFinal['query1NoisyOutput'] = dfFinalQuery1['queryNoisyOutput']
    dfFinal['query2NoisyOutput'] = dfFinalQuery2['queryNoisyOutput']
    print(dfFinal)
    createNestedJSON(dfFinal, 'HAT')
    dfFinal.to_json('../pipelineOutput/' + 'noisyOutput' + '.json')
    return
