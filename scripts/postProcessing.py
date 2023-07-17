import numpy as np
import pandas as pd
import json 
import math
import matplotlib.pyplot as plt

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
        dfFinal['noisyCount'].clip(0, np.inf, inplace = True)
        dfFinal['roundedNoisyCount'] = dfFinal['noisyCount'].round()
        #dfFinal['roundedNoisyCount'].clip(0, np.inf, inplace = True)
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

def RMSEGraph(snr,epsilon,filename):
    alphas=[]
    epsilons=[]
    for i in range(1, 101):
        value = i / 10.0
        alphas.append(value)
        epsilons.append(value*epsilon)
    snr_final=[]
    for alpha in alphas:
        snr_new = []  # Initialize an empty list
        # Iterate over the elements of the original sequence
        for snr_value in snr:
            result = (alpha * alpha) * snr_value  # Perform the multiplication
            snr_new.append(result)
        snr_final.append(snr_new)
    RMSE=[]
    for snr in snr_final:
        rmse=[]
        for value in snr:
            rmse.append(1.0/math.sqrt(value))
        RMSE.append(rmse)
    error_10=[]
    error_25=[]
    error_50=[]
    for rmse in RMSE:
        cnt_10=0
        cnt_25=0
        cnt_50=0
        for value in rmse:
            if value < 0.1:
                cnt_10=cnt_10+1
            if value < 0.25:
                cnt_25=cnt_25+1
            if value < 0.5:
                cnt_50=cnt_50+1
        error_10.append(cnt_10)
        error_25.append(cnt_25)
        error_50.append(cnt_50)
    error_10=np.multiply(np.divide(error_10,len(snr)),100)
    error_25=np.multiply(np.divide(error_25,len(snr)),100)
    error_50=np.multiply(np.divide(error_50,len(snr)),100)
    # Clear the plot before each new plot
    plt.clf()
    plt.plot(epsilons, error_10, color='red', label='x=0.1')
    plt.plot(epsilons, error_25, color='blue', label='x=0.25')
    plt.plot(epsilons, error_50, color='green', label='x=0.5')
    # Add a vertical dotted line at the x position epsilon
    plt.axvline(x=epsilon, linestyle='dotted', color='gray',label=f'ε={epsilon}')
    # Set the plot title and labels
    plt.title('RRMSE graph')
    plt.xlabel('Epsilons')
    plt.ylabel('Percentage of samples whose RMSE error is less than x ')
    plt.legend()
    plt.grid(True)
    # Save the plot to a file (e.g., PNG format)
    plt.savefig('../pipelineOutput/'+filename)

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

def outputFileGenAgg(dict):
    outputFile = '../pipelineOutput/genAggOutput.json'
    nested_json = {}
    for WAYM, df in dict.items():
        nested_json[WAYM] = {}
        for department, counts in df.iterrows():
            nested_json[WAYM][department] = counts.to_dict()
    with open(outputFile, 'w') as fp:
        json.dump(nested_json, fp, indent=4)
    return 

def outputFileCategorical(dfs1, dfs2):
    outputDFs = {}
    outputDFs['Query1']=dfs1
    outputDFs['Query2']=dfs2
    jsonDict = {}
    for query, df in outputDFs.items():
        jsonDict[query] = {}
        for subdistrict, df2 in df.items():
            jsonDict[query][subdistrict] = {}
            for pair, df3 in df2.items():
                jsonDict[query][subdistrict][pair] = df3.to_dict(orient = 'records')
    with open('../pipelineOutput/noisyOutputCategorical.json', 'w') as fp:
        json.dump(jsonDict, fp, indent=4)