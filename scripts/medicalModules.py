# import statements
import pandas as pd
import numpy as np
import scripts.utilities as utils
import logging
# function definitions
###########################
# function to bin the ages
def generalize(dataframe, config, bins):
    attribute_to_generalize = config["k_anonymize"]["generalize"]
    dataframe[f"{config['k_anonymize']['generalize']} Bin"] = pd.cut(
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
    data[f"{config['k_anonymize']['generalize']} Bin"] = pd.cut(data[config['k_anonymize']['generalize']], bins=bin_edges, include_lowest=True)
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
    # epsilon_step = dpConfig["dp_epsilon_step"]
    epsilon_vector = np.array([0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100])
    noise_vector = []
    noisy_query_output = []
    # epsilonVector = np.logspace(-5, 0, 1000)
    output_attribute = dpConfig["dp_output_attribute"]
    if dpConfig["dp_query"] == "count":
        sensitivity = 1
        # computing noise vector for slider implementation in UI (values of noisy output for each value of epsilon)
        for eps in epsilon_vector:
            b_per_eps = sensitivity/eps
            noise_per_eps = np.random.laplace(0,b_per_eps,len(dataframeAccumulate))
            noise_vector.append(noise_per_eps)
        b = sensitivity/epsilon
        # computing bVector for MAE computation
        bVector = sensitivity/epsilon_vector
        bVector = pd.DataFrame(bVector, index=epsilon_vector)
        mean_absolute_error = bVector
        noise = np.random.laplace(0,b,len(dataframeAccumulate))
        privateAggregateDataframe = dataframeAccumulate.copy()
        privateAggregateDataframe["noisy_output"] = privateAggregateDataframe["query_output"] + noise
        # privateAggregateDataframe.drop(columns = ["query_output"], inplace = True)
        for noise in noise_vector:
            noisy_value_vector = privateAggregateDataframe["query_output"] + noise
            noisy_query_output.append(noisy_value_vector)
        noisy_query_output = pd.DataFrame(noisy_query_output)        
        noisy_query_output.index = epsilon_vector
        noisy_query_output = noisy_query_output.rename(columns = dataframeAccumulate[dpConfig["dp_aggregate_attribute"]])
        logging.info("Reached end of med DP")
    elif dpConfig["dp_query"] == "mean":
        # for the mean query we need to compute the noisy sum and the noisy count independently and then divide the noisy sum by the noisy count to find the noisy mean
        sensitivity_count = 1
        sensitivity_sum = dpConfig["dp_max_value_output_attribute"]
        b_count = sensitivity_count/(epsilon/2)
        b_sum = sensitivity_sum/(epsilon/2)
        bVector_count = sensitivity_count/epsilon_vector
        bVector_sum = sensitivity_sum/epsilon_vector
        noise_count = np.random.laplace(0, b_count, len(dataframeAccumulate))
        noise_sum = np.random.laplace(0, b_sum, len(dataframeAccumulate))
        sum = dataframeAccumulate["sum"]
        count = dataframeAccumulate["count"]
        noisy_count = count + noise_count
        noisy_sum = sum + noise_sum
        noisy_mean = noisy_sum/noisy_count
        mae_vector_category = pd.DataFrame()
        noisy_query_output_category = pd.DataFrame()
        for category in dataframeAccumulate[dpConfig["dp_aggregate_attribute"]]:
            sum = dataframeAccumulate.loc[dataframeAccumulate[dpConfig["dp_aggregate_attribute"]] == category, 'sum'].values[0]
            count = dataframeAccumulate.loc[dataframeAccumulate[dpConfig["dp_aggregate_attribute"]] == category, 'count'].values[0]
            mae_vector, noisy_query_output = utils.monte_carlo_sim_mae(10e5, epsilon_vector, bVector_sum, bVector_count, sum, count)
            mae_vector = pd.DataFrame(mae_vector)
            mae_vector_category = pd.concat([mae_vector_category, mae_vector], axis = 1)
            noisy_query_output = pd.DataFrame(noisy_query_output)
            noisy_query_output.rename(columns = {0:category}, inplace = True)
            noisy_query_output_category = pd.concat([noisy_query_output_category, noisy_query_output], axis = 1)
        noisy_query_output = noisy_query_output_category
        noisy_query_output.index = epsilon_vector
        mean_absolute_error = mae_vector_category.mean(axis = 1)
        mean_absolute_error.index=epsilon_vector
        privateAggregateDataframe = dataframeAccumulate.copy()
        privateAggregateDataframe["query_output"] = privateAggregateDataframe["sum"] / privateAggregateDataframe["count"]
        privateAggregateDataframe["noisy_output"] = noisy_mean
        privateAggregateDataframe.drop(columns = ["count", "sum"], inplace = True)
    return privateAggregateDataframe, mean_absolute_error, noisy_query_output
