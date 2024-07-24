# import statements
import pandas as pd
import numpy as np


# function definitions
###########################
# function to bin the ages
def generalize(dataframe, config, bins):
    attribute_to_generalize = config["k_anonymize"]["generalize"]
    dataframe["Age Bin"] = pd.cut(
        dataframe[attribute_to_generalize], bins, ordered=True
    )
    return dataframe

# function to k-anonymize

def k_anonymize(dataframe, config):
    """
    A function to perform k-anonymization on a given dataframe based on the configuration provided.

    Parameters:
    - dataframe: Pandas DataFrame containing the data to be k-anonymized.
    - config: Dictionary containing the configuration settings for k-anonymization.

    Returns:
    - The final bin size after k-anonymization is applied.
    """
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

def user_assignment_k_anonymize(optimal_bin_width, data, config):
    bin_edges = np.arange(config["k_anonymize"]["min_bin_value"], config["k_anonymize"]["max_bin_value"]  + optimal_bin_width, optimal_bin_width)
    data['Age Bin'] = pd.cut(data['Age'], bins=bin_edges, include_lowest=True)
    return data

def medicalDifferentialPrivacy(dataframeAccumulate, configFile):
    """
    Applies differential privacy to a medical dataframe based on the given configuration.

    Parameters:
    - dataframeAccumulate (pandas.DataFrame): The dataframe containing the medical data.
    - configFile (dict): The configuration file containing the differential privacy parameters.

    Returns:
    - privateAggregateDataframe (pandas.DataFrame): The dataframe with differential privacy applied.
    - bVector (pandas.DataFrame): The dataframe containing the b values used for differential privacy.

    This function applies differential privacy to a medical dataframe based on the given configuration. It takes in a dataframe containing the medical data and a configuration file containing the differential privacy parameters. The function then calculates the epsilon vector based on the given epsilon value in the configuration file. 

    If the differential privacy query is "count", the function calculates the sensitivity as 1 and generates the b vector based on the epsilon vector. It then generates random noise using the Laplace distribution and adds it to the "query_output" column of the dataframe. The function drops the "query_output" column and returns the modified dataframe and the b vector.

    If the differential privacy query is "mean", the function calculates the sensitivity for each category in the specified aggregate attribute column. It then generates the b vector based on the sensitivity and epsilon vector. It generates random noise using the Laplace distribution and adds it to the "query_output" column of the dataframe. The function drops the "count" and "query_output" columns and returns the modified dataframe and the b vector.

    Note: The function assumes that the input dataframe contains a "query_output" column.
    """
    dpConfig = configFile['differential_privacy']
    # count = dataframeAccumulate['query_output'].sum()
    epsilon = dpConfig["dp_epsilon"]
    epsilon_step = dpConfig["dp_epsilon_step"]
    epsilonVector = np.arange(0.01,10,epsilon_step).round(2)
    # epsilonVector = np.logspace(-5, 0, 1000)
    output_attribute = dpConfig["dp_output_attribute"]
    if dpConfig["dp_query"] == "count":
        sensitivity = 1
        # no epsilon vector is generated
        b = sensitivity/epsilon
        bVector = sensitivity/epsilonVector
        bVector = pd.DataFrame(bVector, index=epsilonVector)
        noise = np.random.laplace(0,b,len(dataframeAccumulate))
        privateAggregateDataframe = dataframeAccumulate.copy()
        privateAggregateDataframe[f"Noisy {dpConfig['dp_query']}"] = privateAggregateDataframe["query_output"] + noise
        # privateAggregateDataframe.drop(columns = ["query_output"], inplace = True)
    elif dpConfig["dp_query"] == "mean":
        # for the mean query we need to compute the noisy sum and the noisy count indpendently and then divide the noisy sum by the noisy count to find the noisy mean
        sensitivity_count = 1
        sensitivity_sum = dpConfig["dp_max_value_output_attribute"]
        # for category in dataframeAccumulate[dpConfig["dp_aggregate_attribute"]]:
        #     count = dataframeAccumulate.loc[dataframeAccumulate[dpConfig["dp_aggregate_attribute"]] == category,'count']
        #     sum = dataframeAccumulate.loc[dataframeAccumulate[dpConfig["dp_aggregate_attribute"]] == category,'sum']
        #     sensitivity_count.append(1)
        #     sensitivity_sum.append(dpConfig["dp_max_value_output_attribute"])
        # sensitivity_count = np.array(sensitivity_count)
        # sensitivity_sum = np.array(sensitivity_sum)
        b_count = sensitivity_count/(epsilon/2)
        b_sum = sensitivity_sum/(epsilon/2)
        bVector_count = sensitivity_count/epsilonVector
        bVector_sum = sensitivity_sum/epsilonVector
        bVector_count = pd.DataFrame(bVector_count)
        bVector_sum = pd.DataFrame(bVector_sum)
        noise_count = np.random.laplace(0, b_count, len(dataframeAccumulate))
        noise_sum = np.random.laplace(0, b_sum, len(dataframeAccumulate))
        # noise_count = np.array(noise_count).flatten()
        # noise_sum = np.array(noise_sum).flatten()
        noisy_count = dataframeAccumulate["count"] + noise_count
        noisy_sum = dataframeAccumulate["sum"] + noise_sum
        noisy_mean = noisy_sum/noisy_count
        privateAggregateDataframe = dataframeAccumulate.copy()
        privateAggregateDataframe["mean"] = privateAggregateDataframe["sum"] / privateAggregateDataframe["count"]
        privateAggregateDataframe[f"Noisy {output_attribute}"] = noisy_mean
        privateAggregateDataframe.drop(columns = ["count", "sum"], inplace = True)
        bVector = pd.concat([bVector_count, bVector_sum], axis = 1)
        bVector = bVector.sum(axis = 1)
    return privateAggregateDataframe, bVector
