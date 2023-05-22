import numpy as np

def postProcessing(dfNoise, configDict, genType):
    if genType == 'spatio-temporal':
        #postprocessing ITMSQuery1
        globalMaxValue = configDict['globalMaxValue']
        globalMinValue = configDict['globalMinValue']
        dfFinalITMSQuery1 = dfNoise
        dfFinalITMSQuery1['queryNoisyOutput'].clip(globalMinValue, globalMaxValue, inplace = True)
        if configDict['optimized'] == False:
            dfFinalITMSQuery1.drop(['queryOutput'], axis = 1, inplace = True)        
        # #postprocessing ITMS Query 2
        # dfFinalITMSQuery2 = dfNoiseITMSQuery2
        # dfFinalITMSQuery2['query2NoisyOutput'].clip(0, np.inf, inplace = True)
        # dfFinalITMSQuery2.drop(['query2Output'], axis = 1, inplace = True)
        
        return dfFinalITMSQuery1
    
    elif genType == 'categorical':
        dfFinal = dfNoise
        dfFinal['roundedNoisyCount'] = dfFinal['noisyCount'].round()
        dfFinal['roundedNoisyCount'].clip(0, np.inf, inplace = True)
        dfFinal.drop(['noisyCount'], axis = 1, inplace = True)
        return dfFinal

def signalToNoise(snrAverage, configDict):
    # SNR Threshold
    snrUpperLimit = configDict['snrUpperLimit']
    snrLowerLimit = configDict['snrLowerLimit']
    # # snr defined as signal mean over std of noise
    # #signalPower/noisePower
    # # //TODO: possible change to b value instead of laplacian
    # snr = (np.mean(signal*signal))/(bVariance)
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
    print('Your Cumulative Epsilon for the displayed queries is: ' + str(cumulativeEpsilon))
    return cumulativeEpsilon

def outputFile(dfFinal, dataframeName):
    # dataframeName = input('What would you like to name the output dataframe?')
    dfFinal.to_csv('../pipelineOutput/' + dataframeName + '.csv')
    return
