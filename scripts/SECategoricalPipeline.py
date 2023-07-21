import json
import pandas as pd
import preProcessing as premod
import postProcessing as postmod
import categoricalModules as cmod

file_names_list = ['../data/categorical/split_file_0.csv',
              '../data/categorical/split_file_1.csv',
              '../data/categorical/split_file_2.csv',
              '../data/categorical/split_file_3.csv',
              '../data/categorical/split_file_4.csv',
              '../data/categorical/split_file_5.csv',
              '../data/categorical/split_file_6.csv',
              '../data/categorical/split_file_7.csv',
              '../data/categorical/split_file_8.csv',
              '../data/categorical/split_file_9.csv']

configFileName = '../config/SECategoricalConfig.json'
with open(configFileName, "r") as cfile:
    configDict = json.load(cfile)
schemaFileName = 'SECategoricalSchema.json'
premod.schemaValidator(schemaFileName, configFileName)

def chunkHandling(configDict):

    #initializing variables to manage flow of chunks 
    flag = 0
    cnt = 1
    
    #iterate through the chunks and get counts as per query
    for file in file_names_list:
        print('------------------------ Processing chunk no. ', cnt)
        cnt +=1
        dataframe = pd.read_csv(file)    
        dataframe = premod.dropDuplicates(dataframe, configDict)
        dataframe = premod.suppress(dataframe, configDict)
        dataframe = cmod.categoricGeneralization(dataframe, configDict)
    
        if flag == 0:
            query1Dict = cmod.histogramQuery1(dataframe, configDict)
            query2Dict = cmod.histogramQuery2(dataframe, configDict)
            flag = 1
            
        elif flag == 1:
            tempDict = cmod.histogramQuery1(dataframe, configDict)
            tempDict2 = cmod.histogramQuery2(dataframe, configDict)
            query1Dict = cmod.merge_dicts(query1Dict, tempDict)
            query2Dict = cmod.merge_dicts(query2Dict, tempDict2)
    
    return query1Dict, query2Dict
       
def categoricalDP(query1Dict, query2Dict, configDict):
    #compute noise
    chunkedNoiseHistQuery1, bVarianceQuery1 = cmod.noiseComputeHistogramQuery1(query1Dict, configDict)
    chunkedNoiseHistQuery2, bVarianceQuery2 = cmod.noiseComputeHistogramQuery2(query2Dict, configDict)
    
    #compute snr
    cmod.snrQuery(chunkedNoiseHistQuery1, bVarianceQuery1, configDict)
    cmod.snrQuery(chunkedNoiseHistQuery2, bVarianceQuery2, configDict)
    
    #post processing 
    roundedHistQuery1 = cmod.postProcessingQuery(chunkedNoiseHistQuery1, configDict)
    roundedHistQuery2 = cmod.postProcessingQuery(chunkedNoiseHistQuery2, configDict)

    dfFinalQuery1 = cmod.histogramOutputQuery(roundedHistQuery1)
    dfFinalQuery2 = cmod.histogramOutputQuery(roundedHistQuery2)
    
    return dfFinalQuery1, dfFinalQuery2

def outputFileGeneration(dfFinalQuery1, dfFinalQuery2, configDict):
    #final output file generation
    postmod.outputFileCategorical(dfFinalQuery1, dfFinalQuery2, configDict)
    print('\nOutput generated. Please check the pipelineOutput folder.')

# handling the choice for data tapping, including validation of choice
validChoice = 0
while validChoice == 0: 
    print("Select type of output file: \n")
    print('''1. Clean Query Output\n2. Noisy Query Output: \n''')
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
    
    

