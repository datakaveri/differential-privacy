# import statements
import pandas as pd
import numpy as np
import json
import hashlib

# //TODO: Remove functions found in utilities
###########################
# function definitions


# read config
def read_config(configFile):
    with open(configFile, "r") as cfile:
        config = json.load(cfile)
    return config


# read data
def read_data(dataFile):
    with open(dataFile, "r") as dfile:
        data = json.load(dfile)
        dataframe = pd.json_normalize(data)
    return dataframe


# suppress
def suppress(dataframe, config):
    attributes_to_suppress = config["suppress"]
    dataframe.drop(columns=attributes_to_suppress, inplace=True)
    print(dataframe.info())
    return dataframe


# pseudonymize
def pseudonymize(dataframe, config):
    attribute_to_pseudonymize = config["pseudonymize"]
    dataframe["UID"] = (
        dataframe[attribute_to_pseudonymize[0]]
        + dataframe[attribute_to_pseudonymize[1]]
    )
    dataframe["Hashed Value"] = dataframe["UID"].apply(
        lambda x: hashlib.sha256(x.encode()).hexdigest()
    )
    dataframe.drop(columns=["UID"] + attribute_to_pseudonymize, inplace=True)
    return dataframe


###########################
# function to bin the ages
def generalize(dataframe, config, bins):
    attribute_to_generalize = config["generalize"]
    dataframe["Age Bin"] = pd.cut(
        dataframe[attribute_to_generalize], bins, ordered=True
    )
    return dataframe

# function to k-anonymize

# TODO:// Debug (k = 60,70) and add edge case handling
def k_anonymize(dataframe, config):
    # start with each bin size 1
    kConfig = config["k_anonymize"]
    k = kConfig["k"]
    r_count = 1
    # The maximum of the values we are building histogram
    mx_age = np.max(dataframe["Age"])
    flag = 1
    temp_flag = 1
    if (k == dataframe["Count"].sum()):
        return mx_age
    elif (k > dataframe["Count"].sum() or k <= 0):
        print("Please enter a valid value for k, the entered value is :", k)
        return -1
    print("****************************************************************")
    while flag:
        # Calculate number of bins for given r_count (bin size)
        # if mx_age/r_count is not an integer then we put all the
        # Count in partial bin into last but one bin
        # e.g. r_count = 4 mx_age = 26, the number of bins - 6
        # last bin contains counts from last 6 (4 +2, 2 is reminder) entries
        num_bins = int(np.floor(mx_age / r_count))
        reminder = mx_age % r_count
        temp_flag = 1
        # Now we will compute if each bin will satisfy k anonimity or not
        # if it is not satisfied we increment r_count (bin size) and break and
        # create num of bins with updated (increased) binsize

        for b in range(num_bins):
            temp = 0
            eff_rows = r_count
            # For the last bin the number of rows to be considered
            # the rows in the partial bin should be taken care
            if b == (num_bins - 1):
                eff_rows += reminder
            # For each bin we are computing the count (in temp)
            for i in range(1, eff_rows + 1):
                temp += dataframe["Count"][b * r_count + i]
            # if i-th bin does not satisfy k-anonymity, increase the bin size
            # and start afresh with new bin size
            temp = int(temp)
            if (temp < k) and (temp != 0):
                # print("temp : ", temp)
                r_count += 1
                temp_flag = 0
                break
        # control reached here means all bins satisfied k-anonymity and we are setting
        # flag to false and exiting
        if (temp_flag):
            if b * r_count + i >= mx_age:
                for b in range(num_bins):
                    temp = 0
                    eff_rows = r_count
                    if b == (num_bins - 1):
                        eff_rows += reminder
                    for i in range(1, eff_rows + 1):
                        # print(dataframe["Count"][b * r_count + i])
                        temp += dataframe["Count"][b * r_count + i]
                    # print("Bin index : ", b, "Count : ", temp)
                flag = 0
            if num_bins == 1:
                r_count = mx_age
    return r_count

def medicalDifferentialPrivacy(dataframeAccumulate, configFile):
    dpConfig = configFile['differential_privacy']
    # count = dataframeAccumulate['query_output'].sum()
    epsilon = dpConfig["dp_epsilon"]
    epsilonVector = np.arange(0.1,5,epsilon)
    # epsilonVector = np.logspace(-5, 0, 1000)
    output_attribute = dpConfig["dp_output_attribute"]
    if dpConfig["dp_query"] == "count":
        sensitivity = 1
        # no epsilon vector is generated
        b = sensitivity/epsilon
        bVector = sensitivity/epsilonVector
        noise = np.random.laplace(0,b,len(dataframeAccumulate))
        privateAggregateDataframe = dataframeAccumulate.copy()
        privateAggregateDataframe[f"Noisy {output_attribute}"] = privateAggregateDataframe["query_output"] + noise
        privateAggregateDataframe.drop(columns = ["query_output"], inplace = True)
    elif dpConfig["dp_query"] == "mean":
        # count = dataframeAccumulate["count"]
        sensitivity = []
        for category in dataframeAccumulate[dpConfig["dp_aggregate_attribute"]]:
            count = dataframeAccumulate.loc[dataframeAccumulate[dpConfig["dp_aggregate_attribute"]] == category,'count']
            sensitivity.append(dpConfig["dp_max_value_sensitivity"] / count)
        sensitivity = np.array(sensitivity)
        b = sensitivity/epsilon
        bVector = sensitivity/epsilonVector
        noise = [np.random.laplace(0, b, 1) for b in b]
        noise = np.array(noise).flatten()
        privateAggregateDataframe = dataframeAccumulate.copy()
        privateAggregateDataframe[f"Noisy {output_attribute}"] = privateAggregateDataframe["query_output"] + noise
        privateAggregateDataframe.drop(columns = ["count", "query_output"], inplace = True)
    return privateAggregateDataframe, bVector
