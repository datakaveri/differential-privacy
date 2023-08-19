import json
import pandas as pd
import preProcessing as premod
import postProcessing as postmod
import categoricalModules as cmod

'''file_names_list = ['../data/categorical/split_file_0.csv',
              '../data/categorical/split_file_1.csv',
              '../data/categorical/split_file_2.csv',
              '../data/categorical/split_file_3.csv',
              '../data/categorical/split_file_4.csv',
              '../data/categorical/split_file_5.csv',
              '../data/categorical/split_file_6.csv',
              '../data/categorical/split_file_7.csv',
              '../data/categorical/split_file_8.csv',
              '../data/categorical/split_file_9.csv']
'''

with open("../config/categoricalFNList.json", "r") as config_file:
            file_names_list = json.load(config_file)

configFileName = '../config/anonymizationConfig.json'

with open(configFileName, "r") as cfile:
    configDict = json.load(cfile)
configDict = configDict['categorical']
schemaFileName = 'anonymizationSchema.json'

premod.schemaValidator(schemaFileName, configFileName)

def chunkHandling(configDict):

    #initializing variables to manage flow of chunks 
    flag = 0
    cnt = 1
    
    #asking variables
    print('\nUsing numbers select which group by column( Type 0 for default )\n', configDict['groupLocations'], '\n')
    
    while True:
        grouped = int(input())
        if grouped <0 or grouped >len(configDict['groupLocations'])+1:
            print('Invalid column chosen please chose from one of the follows\n', configDict['groupLocations'])
            continue
        break
    
    if grouped == 0:
        groupBy = configDict['queryPer']
    else : 
        groupBy = configDict['groupLocations'][grouped-1]

    
    
    #iterate through the chunks and get counts as per query
    for file in file_names_list:
        print('------------------------ Processing chunk no. ', cnt)
        cnt +=1
        dataframe = pd.read_csv(file)    
        dataframe = premod.dropDuplicates(dataframe, configDict)
        dataframe = premod.suppress(dataframe, configDict)
        dataframe = cmod.categoricGeneralization(dataframe, configDict)
    
        if flag == 0:
            query1Dict = cmod.histogramQuery1(dataframe, configDict, groupBy)            
            query2Dict = cmod.histogramQuery2(dataframe, configDict, groupBy)
            flag = 1
            
        elif flag == 1:
            tempDict = cmod.histogramQuery1(dataframe, configDict, groupBy)
            tempDict2 = cmod.histogramQuery2(dataframe, configDict, groupBy)
            query1Dict = cmod.merge_dicts(query1Dict, tempDict)
            query2Dict = cmod.merge_dicts(query2Dict, tempDict2)
    
    return query1Dict, query2Dict
       
def categoricalDP(query1Dict, query2Dict, configDict):
    #compute noise
    chunkedNoiseHistQuery1, bVarianceQuery1 = cmod.noiseComputeHistogramQuery1(query1Dict, configDict)
    roundedHistQuery1 = cmod.postProcessingQuery(chunkedNoiseHistQuery1, configDict)
    dfFinalQuery1 = cmod.histogramOutputQuery(roundedHistQuery1)
    
    while(True):
        configDict['q2choice'] = int(input('\n\nNoise added successfuly for query1.\n\nPlease choose the method you would like for query2\n1. Noisy Counts\n2. Modal Values\n\n'))
        if configDict['q2choice'] < 0 or configDict['q2choice'] >2:
            continue
        else:
            break
    if configDict['q2choice'] == 2:
        while(True):
            configDict['q2choice'] = int(input('\nPlease choose the method you would like for calculating mode:\n1. Exponential Mechanism\n2. Report Noisy Max\n\n'))+1
            if configDict['q2choice'] < 0 or configDict['q2choice'] > 3:
                continue 
            else :
                break
    if configDict['q2choice'] == 1 or configDict['q2choice'] == 0: 
        chunkedNoiseHistQuery2, bVarianceQuery2 = cmod.noiseComputeHistogramQuery2(query2Dict, configDict)
        roundedHistQuery2 = cmod.postProcessingQuery(chunkedNoiseHistQuery2, configDict)
        dfFinalQuery2 = cmod.histogramOutputQuery(roundedHistQuery2)
    elif configDict['q2choice'] == 2:
        dfFinalQuery2 = cmod.exponentialMechanismHistogramQuery2(query2Dict, configDict)
    elif configDict['q2choice'] == 3:
        dfFinalQuery2 = cmod.reportNoisyMax(query2Dict, configDict)
    
    #compute snr
    if configDict['outputOptions'] ==2:
        snr1=cmod.snrQuery(chunkedNoiseHistQuery1, bVarianceQuery1, configDict)
        postmod.RMSEGraph(snr1,configDict["PrivacyLossBudget"][0],'categorical','Query1')
        print("Relative RMSE Graph generated for query1 check the pipelineOuput Folder")
        if configDict['q2choice'] == 1: 
            snr2=cmod.snrQuery(chunkedNoiseHistQuery2, bVarianceQuery2, configDict)
            postmod.RMSEGraph(snr2,configDict["PrivacyLossBudget"][1],'categorical','Query2')
            print("Relative RMSE Graph generated for query2 check the pipelineOuput Folder")   
    
    return dfFinalQuery1, dfFinalQuery2

def outputFileGeneration(dfFinalQuery1, dfFinalQuery2, configDict):
    #final output file generation
    postmod.outputFileCategorical(dfFinalQuery1, dfFinalQuery2, configDict)
    print('\nOutput generated. Please check the pipelineOutput folder.')

def main():
    # handling the choice for data tapping, including validation of choice
    validChoice = 0
    while validChoice == 0: 
        print("Select type of output file: \n")
        print('''1. Clean Query Output\n2. Noisy Query Output \n''')
        dataTapChoice = int(input("Enter a number to make your selection: "))
        if dataTapChoice == 1:
            configDict['outputOptions'] = 1
            validChoice = 1
        elif dataTapChoice == 2:
            configDict['outputOptions'] = 2
            validChoice = 1
        else:
            print("Choice Invalid, please enter an integer between 1 and 2 to make your choice. \\ ")
    
    query1Dict, query2Dict = chunkHandling(configDict)
    
    if configDict['outputOptions'] == 1:
        outputFileGeneration(query1Dict, query2Dict, configDict)
    elif configDict['outputOptions'] == 2:
        dfFinalQuery1, dfFinalQuery2 = categoricalDP(query1Dict, query2Dict, configDict)
        outputFileGeneration(dfFinalQuery1, dfFinalQuery2, configDict)
    
if __name__ == "__main__":
    main()

