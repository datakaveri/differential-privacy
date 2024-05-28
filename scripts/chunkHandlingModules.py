# import statements
import json
import pandas as pd
import numpy as np
import utilities as utils
import spatioTemporalModules as stmod
import medicalModules as medmod

# function to handle chunked dataframe for pseudonymization and suppression
def chunkHandlingCommon(configDict, operations, fileList):
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    print("Suppressing and Pseudonymizing selected columns")
    for file in fileList:
        lengthList.append(file)
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
    # print(dataframeAccumulate.info())
    return dataframeAccumulate

# function to accumulate chunks with appropriate query building for DP
def chunkAccumulatorSpatioTemporal(dataframeChunk, spatioTemporalConfigDict):
    print("Accumulating chunks for building DP Query")
    dpConfig = spatioTemporalConfigDict
    dataframeAccumulator = dataframeChunk.groupby([dpConfig['dp_aggregate_attribute'][0],\
                                                 dpConfig['dp_aggregate_attribute'][1],\
                                                 dpConfig['dp_aggregate_attribute'][2]])\
                                                .agg(query_output = (dpConfig['dp_output_attribute'], 
                                                             dpConfig['dp_query'])).reset_index()
    return dataframeAccumulator

# function to perform s/t generalization, filtering, query building for chunks
def chunkHandlingSpatioTemporal(spatioTemporalConfigDict, operations, fileList):
# assume that the appropriate config has been selected already based on UI input
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    dataframeAccumulateNew = pd.DataFrame()
    dpConfig = spatioTemporalConfigDict["differential_privacy"]
    print("Performing spatio-temporal generalization and filtering")
    for file in fileList:
        lengthList.append(file)
        print('The chunk number is: ', (len(lengthList)))
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
        dataframeChunk = stmod.spatioTemporalEventFiltering(dataframeChunk, spatioTemporalConfigDict)
        
        # accumulating chunks for dp query building
        dataframeAccumulator = chunkAccumulatorSpatioTemporal(dataframeChunk, dpConfig)
        
        # creating accumulated dataframe
        dataframeAccumulate = pd.concat([dataframeAccumulate, dataframeAccumulator], ignore_index=True)
        print("The length of the accumulate dataframe is: ", len(dataframeAccumulate))    
    print(dataframeAccumulate)
    print(dataframeAccumulate.info())
    # //TODO: Check correct size of accumulate
    print("End of Accumulation")
    return dataframeAccumulate

# function to accumulate chunks with appropriate query building for DP
def chunkAccumulatorMedicalDP(dataframeChunk, medicalConfigDict):
    dpConfig = medicalConfigDict["differential_privacy"]
    print("Accumulating chunks for building DP Query")
    dataframeAccumulator = dataframeChunk.groupby([dpConfig['dp_aggregate_attribute']])\
                                                .agg(query_output = (dpConfig['dp_output_attribute'], 
                                                             dpConfig['dp_query'])).reset_index()
    return dataframeAccumulator

# preprocessing to accumulate chunks for k-anon
def chunkHandlingMedicalKAnon(dataframeChunk, medicalConfigDict):
    chunkHistogram = pd.Series()
    kConfig = medicalConfigDict["k_anonymize"]
    bins = np.arange(kConfig["min_bin_value"], kConfig["max_bin_value"], 1)

    # filling each bin with appropriate count of ages using pd.cut()
    dataframeChunk = medmod.generalize(dataframeChunk, medicalConfigDict, bins)

    # counting no. of users in each bin
    chunkHistogram = dataframeChunk[medicalConfigDict["generalize"]].value_counts().reindex(bins[:-1], fill_value=0)
    return chunkHistogram

# accumulating chunks with appropriate processing
def chunkHandlingMedical(medicalConfigDict, operations, fileList):
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    dataframeAccumulateNew = pd.DataFrame()
    kAnonAccumulate = pd.Series()
    print(medicalConfigDict)
    dpConfig = medicalConfigDict["differential_privacy"]
    kConfig = medicalConfigDict["k_anonymize"]
    for file in fileList:
        lengthList.append(file)
        print('The chunk number is: ', (len(lengthList)))
        with open(file,"r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print('The loaded file is: ' + file + ' with shape ' + str(dataframeChunk.shape))

        # generalizing each chunk
        kAnonChunk = chunkHandlingMedicalKAnon(dataframeChunk, medicalConfigDict)

        # accumulating no. of users per bin for every chunk
        kAnonAccumulate = kAnonAccumulate.add(kAnonChunk, fill_value=0)

        # accumulating chunks for dp query building
        dataframeAccumulator = chunkAccumulatorMedicalDP(dataframeChunk, medicalConfigDict)


        dataframeAccumulate = pd.concat([dataframeAccumulate, dataframeAccumulator], ignore_index=True)
        # print(dataframeAccumulate)
        print("The length of the accumulate dataframe is: ", len(dataframeAccumulate)) 
    
    # concat just adds on rows, so grouping again using the same parameters and computing the query again
    # //TODO: This will only work for mean queries and not count as sum of counts is required
    if dpConfig["dp_query"] == "mean":
        dataframeAccumulate = dataframeAccumulate.groupby([dpConfig['dp_aggregate_attribute']])\
                                                .agg(query_output = ('query_output', \
                                                             dpConfig['dp_query'])).reset_index()
    elif dpConfig["dp_query"] == "count":
        dpAccumulate = dataframeAccumulate.groupby([dpConfig['dp_aggregate_attribute']])\
                                                .agg(query_output = ('query_output', \
                                                             "sum")).reset_index()
    print(dpAccumulate) 
    print(dpAccumulate.info())

    # reassigning column names
    kAnonAccumulate = pd.DataFrame({medicalConfigDict["generalize"]:kAnonAccumulate.index, 'Count':kAnonAccumulate.values})

# //TODO: Add in k-anonymity implementation for chunked data
# //TODO: Add in DP implementation for medical queries
    return kAnonAccumulate