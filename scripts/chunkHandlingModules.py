# import statements
import json
import pandas as pd
import numpy as np
import scripts.utilities as utils
import scripts.spatioTemporalModules as stmod
import scripts.medicalModules as medmod
import logging

# select logging level
logging.basicConfig(level = logging.INFO)

# function to handle chunked dataframe for pseudonymization and suppression
def chunkHandlingCommon(configDict, operations, fileList):
    """
    Perform common operations on a list of JSON files and return a single DataFrame.

    Args:
        configDict (dict): A dictionary containing configuration parameters.
        operations (list): A list of strings specifying the operations to perform.
        fileList (list): A list of file paths to the JSON files.

    Returns:
        pandas.DataFrame: A DataFrame containing the accumulated data from all the JSON files.

    Raises:
        None

    Description:
        This function reads a list of JSON files, performs common operations on each chunk of data, and accumulates the results into a single DataFrame. 

        The operations performed include:
        - Suppressing and pseudonymizing selected columns.

        The function iterates over each file in the `fileList`, loads the JSON data into a DataFrame, and performs the following operations:
        - Dropping duplicates.
        - Suppressing columns if "suppress" is present in the `operations` list.
        - Pseudonymizing columns if "pseudonymize" is present in the `operations` list.

        The resulting DataFrame is accumulated into the `dataframeAccumulate` DataFrame using `pd.concat`.

        The final DataFrame is returned.

    """
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    logging.info("Suppressing or Pseudonymizing selected columns")
    for file in fileList:
        lengthList.append(file)
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            logging.info(
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
            logging.info("Performing Attribute Suppression for chunk " + str(len(lengthList)))
        
        # pseudonymizing columns
        if "pseudonymize" in operations:
            dataframeChunk = utils.pseudonymize(dataframeChunk, configDict)
            logging.info("Performing Attribute Pseudonymization for chunk " + str(len(lengthList)))

        dataframeAccumulate = pd.concat(
            [dataframeAccumulate, dataframeChunk], ignore_index=True
        )
    # print(dataframeAccumulate.info())
    return dataframeAccumulate

def chunkAccumulatorSpatioTemporal(dataframeChunk, spatioTemporalConfigDict):
    """
    Accumulates chunks for building a DP query.

    Args:
        dataframeChunk (pandas.DataFrame): The chunked dataframe to be accumulated.
        spatioTemporalConfigDict (dict): A dictionary containing the configuration for spatio-temporal generalization and filtering.

    Returns:
        pandas.DataFrame: The accumulated dataframe for building the DP query.

    This function groups the dataframeChunk by the attributes specified in the spatioTemporalConfigDict and performs aggregations on the specified output attribute. The resulting dataframe contains the sum, count, and maximum values of the output attribute for each group. The function logs a message indicating the start of the accumulation process.

    """
    logging.info("Accumulating chunks for building DP Query")
    dpConfig = spatioTemporalConfigDict
    groupby_attributes = dpConfig["dp_aggregate_attribute"]

    dataframeAccumulator = dataframeChunk.groupby(groupby_attributes).agg(
    output_attribute_sum=(dpConfig["dp_output_attribute"], 'sum'),
    output_attribute_count=(dpConfig["dp_output_attribute"], 'count'),
    output_attribute_max = (dpConfig["dp_output_attribute"], 'max')).reset_index()

    # print(dataframeAccumulator)
    return dataframeAccumulator

def queryBuilderSpatioTemporal(dfAccumulateCombined, dpConfig):
    """
    Builds a query based on the given DataFrame `dfAccumulateCombined` and `dpConfig` for spatial-temporal data.

    Parameters:
        dfAccumulateCombined (pandas.DataFrame): The DataFrame containing the accumulated spatial-temporal data.
        dpConfig (dict): The configuration dictionary for the differential privacy query.

    Returns:
        pandas.DataFrame: The DataFrame with the query results.

    Description:
        This function builds a query based on the given DataFrame `dfAccumulateCombined` and `dpConfig` for spatial-temporal data.
        The query is determined by the value of `dpConfig["dp_query"]`.

        If `dpConfig["dp_query"]` is "mean", the function performs the following steps:
        1. Group the DataFrame by ['HAT', 'license_plate'] and calculate the sum of counts per license plate.
        2. Group the DataFrame by 'HAT' and calculate the maximum of the sum of counts per license plate.
        3. Group the DataFrame by 'HAT' and calculate the sum of counts for all license plates and Dates.
        4. Calculate the sum of sums for all license plates and Dates.
        5. Calculate the mean of the sum of sums divided by the sum of counts.
        6. Add the maximum of sum of counts per license plate as a new column.

        If `dpConfig["dp_query"]` is "count", the function performs the following steps:
        1. Group the DataFrame by ['HAT', 'Date'] and calculate the count of license plates where the max speed value is greater than the user defined threshold value.
        2. Group the DataFrame by 'HAT' and calculate the mean of the count of license plate max values across all days.
        3. Rename the 'mean_of_counts' column to 'query_output'.

        The resulting DataFrame contains the query results.

    """
    if dpConfig["dp_query"] == "mean":
            dfSensitivity = dfAccumulateCombined.groupby(['HAT', 'license_plate']).agg(
                                                sum_of_counts_per_lp=('output_attribute_count', 'sum') #sum of counts per license plate
                                                ).reset_index()
            
            dfSensitivity = dfSensitivity.groupby('HAT').agg(
                                                max_of_sum_of_counts_per_lp=('sum_of_counts_per_lp', 'max') # max of sum of counts per license plate
                                                ).reset_index()
            
            # mean of speed values per HAT
            dfAccumulateCombined = dfAccumulateCombined.groupby('HAT').agg(
                                                            sum_of_counts=('output_attribute_count','sum'), # sum of counts for all license plates, Dates
                                                            sum_of_sums=('output_attribute_sum','sum') # sum of sums for all license plates, Dates
                                                            ).reset_index()

            dfAccumulateCombined['query_output'] = dfAccumulateCombined['sum_of_sums']/dfAccumulateCombined['sum_of_counts']
            dfAccumulateCombined['max_of_sum_of_counts_per_lp'] = dfSensitivity['max_of_sum_of_counts_per_lp']
    elif dpConfig["dp_query"] == "count":
        # count of license plates per HAT per Date for which the max speed value is greater than the user defined threshold value
        dfAccumulateCombined = dfAccumulateCombined.groupby(['HAT', 'Date']).agg(
                                                        count_of_license_plates=('output_attribute_max', lambda x: (x > dpConfig['dp_query_value_threshold']).sum())
                                                        ).reset_index()
        # logging.debug("dfAC after threshold enforced"+ str(dfAccumulateCombined))
        # logging.debug(str(len(dfAccumulateCombined)))

        # taking the mean across all days of the count of license plates per 'HAT' combination where the maximum value is greater than a threshold
        dfAccumulateCombined = dfAccumulateCombined.groupby(['HAT']).agg(
                                                        mean_of_counts=('count_of_license_plates', 'mean') # mean of counts of license plate max values across all days
                                                        ).reset_index()
        dfAccumulateCombined.rename(columns={'mean_of_counts':'query_output'}, inplace = True)

        # logging.debug("dfAC mean across all days", dfAccumulateCombined)
        # logging.debug("The length of the accumulate dataframe is: "+ str(len(dfAccumulateCombined)))
    return dfAccumulateCombined

def chunkHandlingSpatioTemporal(spatioTemporalConfigDict, fileList):
    """
    Handles the spatio-temporal generalization and filtering of chunks of data.

    Args:
        spatioTemporalConfigDict (dict): A dictionary containing the configuration for spatio-temporal generalization and filtering.
        fileList (list): A list of file paths representing the chunks of data to be processed.

    Returns:
        tuple: A tuple containing the combined and filtered dataframe of accumulated chunks and the time range of the data.

    """
    # assume that the appropriate config has been selected already based on UI input
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    startDay, endDay = [], []
    dpConfig = spatioTemporalConfigDict["differential_privacy"]
    logging.info("Performing spatio-temporal generalization and filtering")
    for file in fileList:
        lengthList.append(file)
        logging.info("The chunk number is: "+ str(len(lengthList)))
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            logging.info(
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

        # # filtering HATS by average number of events per day
        # dataframeChunk = stmod.spatioTemporalEventFiltering(
        #     dataframeChunk, spatioTemporalConfigDict
        # )

        # accumulating chunks for dp query building
        dataframeAccumulator = chunkAccumulatorSpatioTemporal(dataframeChunk, dpConfig)
    
        # creating accumulated dataframe
        dataframeAccumulate = pd.concat(
            [dataframeAccumulate, dataframeAccumulator], ignore_index=True
        )
        # print(dataframeAccumulate)
        # aggregating to combine colection of discrete dataframe objects created by pd.concat
        dataframeAccumulateAgg = dataframeAccumulate.groupby(dpConfig["dp_aggregate_attribute"]).agg(
                                                        output_attribute_count=('output_attribute_count','sum'), # sum of counts
                                                        output_attribute_sum=('output_attribute_sum','sum'), # sum of sums
                                                        output_attribute_max=('output_attribute_max','max') # max of max
                                                        ).reset_index()
        
        dataframeAccumulate = pd.DataFrame()
        dataframeAccumulate = dataframeAccumulateAgg

    # print(dfAccumulateCombined)

    dfAccumulateCombined = dataframeAccumulate

    # filtering HATS by average number of events per day
    dfAccumulateCombined = stmod.spatioTemporalEventFiltering(
        dfAccumulateCombined, spatioTemporalConfigDict
    )
    
    dfAccumulateCombined = queryBuilderSpatioTemporal(dfAccumulateCombined, dpConfig)
    
    timeRange = 1 + (max(endDay) - min(startDay)).days    
    
    # uncomment for testing
    # print(dataframeAccumulate)
    # print(dfAccumulateCombined)
    # print(dataframeAccumulate.info())
    # print(dataframeCountAccumulate)

    logging.info("End of Accumulation")
    return dfAccumulateCombined, timeRange

# function to accumulate chunks with appropriate query building for DP
def chunkAccumulatorMedicalDP(dataframeChunk, medicalConfigDict):
    dpConfig = medicalConfigDict["differential_privacy"]
    logging.info("Accumulating chunks for building DP Query")
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
        dataframeChunk[medicalConfigDict["k_anonymize"]["generalize"]]
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
    # print(medicalConfigDict)
    # dpConfig = medicalConfigDict["differential_privacy"]
    # kConfig = medicalConfigDict["k_anonymize"]
    for file in fileList:
        lengthList.append(file)
        logging.info("The chunk number is: "+ str(len(lengthList)))
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            logging.info(
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
            medicalConfigDict["k_anonymize"]["generalize"]: kAnonAccumulate.index,
            "Count": kAnonAccumulate.values,
        }
    )
    return kAnonAccumulate

def chunkHandlingMedicalDP(medicalConfigDict, fileList):
    lengthList = []
    dataframeAccumulate = pd.DataFrame()
    # print(medicalConfigDict)
    dpConfig = medicalConfigDict["differential_privacy"]
    for file in fileList:
        lengthList.append(file)
        logging.info("#########################################")
        logging.info("The chunk number is: "+ str(len(lengthList)))
        with open(file, "r") as dfile:
            dataDict = json.load(dfile)
            dataframeChunk = pd.json_normalize(dataDict)
            logging.info(
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
        logging.info("The length of the accumulate dataframe is: " + str(len(dataframeAccumulate)))

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
    # print(dpAccumulate)
    # print(dpAccumulate.info())
    return dpAccumulate
