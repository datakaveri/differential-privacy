# import statements
import json
import pandas as pd
import numpy as np
import scripts.utilities as utils
import scripts.spatioTemporalModules as stmod
import scripts.medicalModules as medmod


# function to handle chunked dataframe for pseudonymization and suppression
def chunkHandlingCommon(configDict, operations, fileList):
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    print("Suppressing and Pseudonymizing selected columns")
    for file in fileList:
        lengthList.append(file)
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print(
                "The loaded file is: "
                + file
                + " with shape "
                + str(dataframeChunk.shape)
            )

        # dropping duplicates
        dataframeChunk = utils.deduplicate(dataframeChunk)

        # supressing columns
        if "suppress" in operations:
            dataframeChunk = utils.suppress(dataframeChunk, configDict)
            print("Performing Attribute Suppression for chunk ", len(lengthList))
        
        # pseudonymizing columns
        if "pseudonymize" in operations:
            dataframeChunk = utils.pseudonymize(dataframeChunk, configDict)
            print("Performing Attribute Pseudonymization for chunk ", len(lengthList))

        dataframeAccumulate = pd.concat(
            [dataframeAccumulate, dataframeChunk], ignore_index=True
        )
    # print(dataframeAccumulate.info())
    return dataframeAccumulate

# TODO: Implement fixed query choices only
# function to accumulate chunks with appropriate query building for DP
def chunkAccumulatorSpatioTemporal(dataframeChunk, spatioTemporalConfigDict):
    print("Accumulating chunks for building DP Query")
    dpConfig = spatioTemporalConfigDict
    groupby_attributes = dpConfig["dp_aggregate_attribute"]

    dataframeAccumulator = dataframeChunk.groupby(groupby_attributes).agg(
    output_attribute_sum=(dpConfig["dp_output_attribute"], 'sum'),
    output_attribute_count=(dpConfig["dp_output_attribute"], 'count'),
    output_attribute_max = (dpConfig["dp_output_attribute"], 'max')).reset_index()

    # print(dataframeAccumulator)
    return dataframeAccumulator

# function to perform s/t generalization, filtering, query building for chunks
def chunkHandlingSpatioTemporal(spatioTemporalConfigDict, fileList):
    # assume that the appropriate config has been selected already based on UI input
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    dataframeCountAccumulate = pd.DataFrame()
    startDay, endDay = [], []
    dpConfig = spatioTemporalConfigDict["differential_privacy"]
    print("Performing spatio-temporal generalization and filtering")
    for file in fileList:
        lengthList.append(file)
        print("The chunk number is: ", (len(lengthList)))
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print(
                "The loaded file is: "
                + file
                + " with shape "
                + str(dataframeChunk.shape)
            )

        # creating H3 index
        dataframeChunk = stmod.spatialGeneralization(
            dataframeChunk, spatioTemporalConfigDict
        )

        # creating timeslots
        dataframeChunk = stmod.temporalGeneralization(
            dataframeChunk, spatioTemporalConfigDict
        )

        # creating HATs from H3 and timeslots
        dataframeChunk = stmod.HATcreation(dataframeChunk)

        # filtering time slots by start and end time
        dataframeChunk = stmod.temporalEventFiltering(
            dataframeChunk, spatioTemporalConfigDict
        )
        
        # update the max and min dates
        startDay.append(dataframeChunk['Date'].min())
        endDay.append(dataframeChunk['Date'].max())

        # filtering HATS by average number of events per day
        dataframeChunk = stmod.spatioTemporalEventFiltering(
            dataframeChunk, spatioTemporalConfigDict
        )

        # accumulating chunks for dp query building
        dataframeAccumulator = chunkAccumulatorSpatioTemporal(dataframeChunk, dpConfig)
    
        # creating accumulated dataframe
        dataframeAccumulate = pd.concat(
            [dataframeAccumulate, dataframeAccumulator], ignore_index=True
        )
        # print(dataframeAccumulate)

    # aggregating to combine colection of discrete dataframe objects created by pd.concat
    dfAccumulateCombined = dataframeAccumulate.groupby(dpConfig["dp_aggregate_attribute"]).agg(
                                                        counts=('output_attribute_count','sum'), # sum of counts
                                                        sums=('output_attribute_sum','sum'), # sum of sums
                                                        max=('output_attribute_max','max') # max of max
                                                        ).reset_index()
    # print(dfAccumulateCombined)
    
    if dpConfig["dp_query"] == "mean":
        # mean of speed values per HAT
        dfAccumulateCombined = dfAccumulateCombined.groupby('HAT').agg(
                                                        sum_of_counts=('counts','sum'), # sum of sum of counts for all license plates, Dates
                                                        sum_of_sums=('sums','sum') # sum of sum of sums for all license plates, Dates
                                                        ).reset_index()
        dfAccumulateCombined['query_output'] = dfAccumulateCombined['sum_of_sums']/dfAccumulateCombined['sum_of_counts']
        max_count = dfAccumulateCombined["sum_of_counts"].max()
        print('max_count', max_count)
        
    elif dpConfig["dp_query"] == "count":
        # count of license plates per HAT per Date for which the max speed value is greater than the user defined threshold value

        dfAccumulateCombined = dfAccumulateCombined.groupby(['HAT', 'Date']).agg(
                                                        count_of_license_plates=('max', lambda x: (x > dpConfig['dp_query_value_threshold']).sum())
                                                        ).reset_index()
        # print("dfAC after threshold enforced", dfAccumulateCombined)
        # print(len(dfAccumulateCombined))

        # taking the mean across all days of the count of license plates per 'HAT' combination where the maximum value is greater than a threshold
        dfAccumulateCombined = dfAccumulateCombined.groupby(['HAT']).agg(
                                                        mean_of_count_of_license_plates=('count_of_license_plates', 'mean')
                                                        ).reset_index()
        # print("dfAC mean across all days", dfAccumulateCombined)
        # print("The length of the accumulate dataframe is: ", len(dfAccumulateCombined))
                    
    timeRange = 1 + (max(endDay) - min(startDay)).days    
    
    # uncomment for testing
    # print(dataframeAccumulate)
    # print(dfAccumulateCombined)
    # print(dataframeAccumulate.info())
    # print(dataframeCountAccumulate)

    print("End of Accumulation")
    return dfAccumulateCombined, timeRange, max_count

# function to accumulate chunks with appropriate query building for DP
def chunkAccumulatorMedicalDP(dataframeChunk, medicalConfigDict):
    dpConfig = medicalConfigDict["differential_privacy"]
    print("Accumulating chunks for building DP Query")
    dataframeAccumulator = (
        dataframeChunk.groupby([dpConfig["dp_aggregate_attribute"]])
        .agg(query_output=(dpConfig["dp_output_attribute"], dpConfig["dp_query"]),
             count=(dpConfig["dp_output_attribute"], "count"))
        .reset_index()
    )
    return dataframeAccumulator

# preprocessing to accumulate chunks for k-anon
def chunkAccumulatorMedicalKAnon(dataframeChunk, medicalConfigDict):
    chunkHistogram = pd.Series()
    kConfig = medicalConfigDict["k_anonymize"]
    bins = np.arange(kConfig["min_bin_value"], kConfig["max_bin_value"], 1)

    # filling each bin with appropriate count of ages using pd.cut()
    dataframeChunk = medmod.generalize(dataframeChunk, medicalConfigDict, bins)

    # counting no. of users in each bin
    chunkHistogram = (
        dataframeChunk[medicalConfigDict["generalize"]]
        .value_counts()
        .reindex(bins[:-1], fill_value=0)
    )
    return chunkHistogram

# accumulating chunks with appropriate processing for KAnon
def chunkHandlingMedicalKAnon(medicalConfigDict, fileList):
    lengthList = []
    # dataframeAccumulate = pd.DataFrame()
    # dataframeAccumulateNew = pd.DataFrame()
    kAnonAccumulate = pd.Series()
    print(medicalConfigDict)
    # dpConfig = medicalConfigDict["differential_privacy"]
    # kConfig = medicalConfigDict["k_anonymize"]
    for file in fileList:
        lengthList.append(file)
        print("The chunk number is: ", (len(lengthList)))
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print(
                "The loaded file is: "
                + file
                + " with shape "
                + str(dataframeChunk.shape)
            )

        # generalizing each chunk
        kAnonChunk = chunkAccumulatorMedicalKAnon(dataframeChunk, medicalConfigDict)

        # accumulating no. of users per bin for every chunk
        kAnonAccumulate = kAnonAccumulate.add(kAnonChunk, fill_value=0)

    # reassigning column names
    kAnonAccumulate = pd.DataFrame(
        {
            medicalConfigDict["generalize"]: kAnonAccumulate.index,
            "Count": kAnonAccumulate.values,
        }
    )
    return kAnonAccumulate

def chunkHandlingMedicalDP(medicalConfigDict, fileList):
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    print(medicalConfigDict)
    dpConfig = medicalConfigDict["differential_privacy"]
    for file in fileList:
        lengthList.append(file)
        print("The chunk number is: ", (len(lengthList)))
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            print(
                "The loaded file is: "
                + file
                + " with shape "
                + str(dataframeChunk.shape)
            )

        # accumulating chunks for dp query building
        dataframeAccumulator = chunkAccumulatorMedicalDP(
            dataframeChunk, medicalConfigDict
        )

        dataframeAccumulate = pd.concat(
            [dataframeAccumulate, dataframeAccumulator], ignore_index=True
        )
        # print(dataframeAccumulate)
        print("The length of the accumulate dataframe is: ", len(dataframeAccumulate))

    # concat just adds on rows, so grouping again using the same parameters and computing the query again
    if dpConfig["dp_query"] == "mean":
        # print("test", dataframeAccumulate)
        dpAccumulate = (
            dataframeAccumulate.groupby([dpConfig["dp_aggregate_attribute"]])
            .agg(query_output=("query_output", dpConfig["dp_query"]),
                 count=("count", "sum"))
            .reset_index()
        )
    elif dpConfig["dp_query"] == "count":
        dpAccumulate = (
            dataframeAccumulate.groupby([dpConfig["dp_aggregate_attribute"]])
            .agg(query_output=("query_output", "sum"))
            .reset_index()
        )
    print(dpAccumulate)
    print(dpAccumulate.info())
    return dpAccumulate
