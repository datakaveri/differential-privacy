# import statements
import json
import pandas as pd
import utilities as utils

fileList = ['../data/syntheticMedicalChunks/medical_data_split_file_0.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_1.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_2.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_3.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_4.json']

# for testing
operations = ["suppress", "pseudonymize"]
configDict = utils.read_config("../config/pipelineConfig.json")
medicalConfigDict = configDict[configDict["data_type"]]
print(medicalConfigDict)

# function to handle chunked dataframe for pseudonymization and suppression
def chunkHandlingCommon(configDict, operations, fileList):
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    for file in fileList:
        lengthList.append(file)
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframeChunk.shape))
            # configDict['dataFile'] == file

        #dropping duplicates
        dataframeChunk = utils.deduplicate(dataframeChunk)

        #supressing columns
        if "suppress" in operations:
            dataframeChunk = utils.suppress(dataframeChunk, configDict)
        
        # pseudonymizing columns
        if "pseudonymize" in operations:
            dataframeChunk = utils.pseudonymize(dataframeChunk, configDict)
        
    dataframeAccumulate = pd.concat([dataframeAccumulate, dataframeChunk], ignore_index=True)
    print(dataframeAccumulate.info())
    return dataframeAccumulate

chunkHandlingCommon(medicalConfigDict, operations, fileList)

def chunkHandlingSpatioTemporal():

    return



def chunkHandlingMedical():

    return