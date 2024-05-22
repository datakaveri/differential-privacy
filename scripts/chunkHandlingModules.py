# import statements
import json
import pandas as pd
import utilities as utils
import spatioTemporalModules as stmod

# for testing
medicalFileList = ['../data/syntheticMedicalChunks/medical_data_split_file_0.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_1.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_2.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_3.json',
            '../data/syntheticMedicalChunks/medical_data_split_file_4.json']

# for testing
spatioTemporalFileList = ['../data/spatioTemporalChunks/split_file_0.json',
            '../data/spatioTemporalChunks/split_file_1.json',
            '../data/spatioTemporalChunks/split_file_2.json',
            '../data/spatioTemporalChunks/split_file_3.json',
            '../data/spatioTemporalChunks/split_file_4.json',
            '../data/spatioTemporalChunks/split_file_5.json',
            '../data/spatioTemporalChunks/split_file_6.json',
            '../data/spatioTemporalChunks/split_file_7.json',
            '../data/spatioTemporalChunks/split_file_8.json',
            '../data/spatioTemporalChunks/split_file_9.json']

operations = ["suppress", "pseudonymize"]
configDict = utils.read_config("../config/pipelineConfig.json")
medicalConfigDict = configDict["medical"]
spatioTemporalConfigDict = configDict["spatioTemporal"]


# function to handle chunked dataframe for pseudonymization and suppression
def chunkHandlingCommon(configDict, operations, fileList):
    # lengthList = []
    dataframeAccumulate = pd.DataFrame()
    for file in fileList:
        # lengthList.append(file)
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframeChunk.shape))

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

# chunkHandlingCommon(medicalConfigDict, operations, medicalFileList)

def chunkHandlingSpatioTemporal(spatioTemporalConfigDict, operations, fileList):
# assume that the appropriate config has been selected already based UI input
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    dataframeAccumulateNew = pd.DataFrame()
    dpConfig = spatioTemporalConfigDict["differential_privacy"]
    for file in fileList:
        lengthList.append(file)
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframeChunk.shape))

        # creating H3 index
        dataframeChunk = stmod.spatialGeneralization(dataframeChunk, spatioTemporalConfigDict)

        # creating timeslots
        dataframeChunk = stmod.temporalGeneralization(dataframeChunk, spatioTemporalConfigDict)

        # creating HATs from H3 and timeslots
        dataframeChunk = stmod.HATcreation(dataframeChunk)

        # filtering time slots by start and end time
        dataframeChunk = stmod.temporalEventFiltering(dataframeChunk, spatioTemporalConfigDict)

        # filtering HATS by average number of events per day
        dataframeChunk = stmod. spatioTemporalEventFiltering(dataframeChunk, spatioTemporalConfigDict)

        print(dataframeChunk)
        
        # if "mean" in spatioTemporalConfigDict["dp_query"]:
        # accumulating chunks for count and mean query
        dataframeAccumulate = dataframeChunk.groupby([dpConfig['dp_aggregate_attribute'][0],\
                                                 dpConfig['dp_aggregate_attribute'][1],\
                                                 dpConfig['dp_aggregate_attribute'][2]])\
                                                .agg(mean = (dpConfig['dp_output_attribute'], 
                                                             dpConfig['dp_query']))
        
        dataframeAccumulateNew = pd.concat([dataframeAccumulate, dataframeAccumulateNew], ignore_index=True)
    print(dataframeAccumulateNew)
    # //TODO: Add DP implementation
    return dataframeAccumulate

chunkHandlingSpatioTemporal(spatioTemporalConfigDict, operations, spatioTemporalFileList)




def chunkHandlingMedical():
# //TODO: Add in k-anonymity implementation for chunked data
# //TODO: Add in DP implementation for medical queries
    return