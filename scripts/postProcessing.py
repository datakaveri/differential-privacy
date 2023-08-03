import numpy as np
import pandas as pd
import json 
import math
import matplotlib.pyplot as plt

def postProcessingSpatioTemporal(dfNoise, configDict):
    #postprocessing ITMSQuery1
    globalMaxValue = configDict['globalMaxValue']
    globalMinValue = configDict['globalMinValue']
    if dfNoise.name == 'dfFinalQuery1':
        dfFinalITMSQuery1 = dfNoise
        dfFinalITMSQuery1['queryNoisyOutput'].clip(globalMinValue, globalMaxValue, inplace = True)
        # dfFinalITMSQuery1.drop(['queryOutput'], axis = 1, inplace = True)
        return dfFinalITMSQuery1
    elif dfNoise.name == 'dfFinalQuery2':
        dfFinalITMSQuery2 = dfNoise
        dfFinalITMSQuery2['queryNoisyOutput'].clip(0, np.inf, inplace = True)
        # dfFinalITMSQuery2.drop(['queryOutput'], axis = 1, inplace = True)
        return dfFinalITMSQuery2

def postProcessingCategorical(dfNoise):
        dfFinal = dfNoise
        dfFinal['noisyCount'].clip(0, np.inf, inplace = True)
        dfFinal['roundedNoisyCount'] = dfFinal['noisyCount'].round()
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
    #computing the cumulative epsilon
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

def outputFileSpatioTemporal(dataTapChoice, dfFinalQuery1, dfFinalQuery2 = None):
    if dataTapChoice == 1:
        # dfFinal = dfFinalQuery1.to_dict(orient='records')
        dfFinal = dfFinalQuery1.copy()
        # dfFinal['Date'] = pd.to_datetime(dfFinal['Date']).astype(str)
        dfFinal['Date'] = dfFinalQuery1['Date'].astype(str)
        grouped_data = dfFinal.groupby(['Date', 'license_plate']).apply(lambda x: x.drop(['Date', 'license_plate'], axis=1).to_dict('records')).reset_index(name='data')
        nested_json_data = grouped_data.groupby('Date').apply(lambda x: x.set_index('license_plate')['data'].to_dict()).to_dict()
        outputFile = '../pipelineOutput/pseudonymizedAggOutputSpatioTemporal.json'
        with open(outputFile, 'w') as file:
            json.dump(nested_json_data, file, indent=4)
        print('Pseudonymized and aggregated query output generated. Please check the pipelineOutput folder.')
        print('\n################################################################\n')
    elif dataTapChoice == 2:
        dfFinal = pd.DataFrame()
        dfFinal['HAT'] = dfFinalQuery1['HAT']
        dfFinal['query1CleanOutput'] = dfFinalQuery1['queryOutput']
        dfFinal['query2CleanOutput'] = dfFinalQuery2['queryOutput']
        dfFinal = createNestedJSON(dfFinal, 'HAT')
        outputFile = '../pipelineOutput/cleanOutputSpatioTemporal.json'
        with open(outputFile, 'w') as file:
            json.dump(dfFinal, file, indent=4)
        print('Clean query output generated. Please check the pipelineOutput folder.')
        print('\n################################################################\n')
    elif dataTapChoice == 3:
        dfFinal = pd.DataFrame()
        dfFinal['HAT'] = dfFinalQuery1['HAT']
        dfFinal['query1NoisyOutput'] = np.round(dfFinalQuery1['queryNoisyOutput'], 3)
        dfFinal['query2NoisyOutput'] = np.round(dfFinalQuery2['queryNoisyOutput'], 3)
        dfFinal = createNestedJSON(dfFinal, 'HAT')
        outputFile = '../pipelineOutput/noisyOutputSpatioTemporal.json'
        with open(outputFile, 'w') as file:
            json.dump(dfFinal, file, indent=4)
        print('Differentially private query output generated. Please check the pipelineOutput folder.')
        print('\n################################################################\n')
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

def outputFileCategorical(dfs1, dfs2, configDict):
    outputDFs = {}
    outputDFs['Query1']=dfs1
    outputDFs['Query2']=dfs2
    if configDict['outputOptions'] == 1:
        name = 'cleanOutputCategorical.json'
    elif configDict['outputOptions'] == 2:
        name = 'noisyOutputCategorical.json'
    
    jsonDict = {}
    for query, df in outputDFs.items():
        jsonDict[query] = {}
        for subdistrict, df2 in df.items():
            jsonDict[query][subdistrict] = {}
            for pair, df3 in df2.items():
                jsonDict[query][subdistrict][pair] = df3.to_dict(orient = 'records')
    with open('../pipelineOutput/'+name, 'w') as fp:
        json.dump(jsonDict, fp, indent=4)

def RMSEGraph(snr,epsilon,type,Query):
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
            if value==0:
                rmse.append(pow(10,10)*0.2)
            else:
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
    plt.axvline(x=epsilon, linestyle='dotted', color='gray',label=f'chosen ε={epsilon}')
    # Set the plot title and labels
    plt.title('Relative RMSE graph For '+Query)
    plt.xlabel('Epsilons')
    plt.ylabel('Percentage of samples whose Relative RMSE  is less than x')
    plt.legend()
    plt.grid(True)
    # Save the plot to a file (e.g., PNG format)
    filename=type+Query+'.png'
    plt.savefig('../pipelineOutput/'+filename)
    return