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
# functions for k-anonymity


# function to bin the ages
def generalize(dataframe, config, bins):
    attribute_to_generalize = config["generalize"]
    dataframe["Age Bin"] = pd.cut(
        dataframe[attribute_to_generalize], bins, ordered=True
    )
    return dataframe

###############################################################################
# Input : The dataframe with frequency counts for each possible age and k
# output : width of the each bin, r_count
# If the number of bins is not an integer the last bins are being merged
# Please see the effec_rows variable
###############################################################################
def k_anonymize(dataframe, config):
    # start with each bin size 1
    kConfig = config["k_anonymize"]
    k = kConfig["k"]
    r_count = 1
    # The maximum of the values we are building histogram
    mx_age = np.max(dataframe["Age"])
    flag = 1
    print("****************************************************************")
    while flag:
        # Calculate number of bins for given r_count (bin size)
        # if mx_age/r_count is not an integer then we put all the
        # Count in partial bin into last but one bin
        # e.g. r_count = 4 mx_age = 26, the number of bins - 6
        # last bin contains counts from last 6 (4 +2, 2 is reminder) entries
        num_bins = int(np.floor(mx_age / r_count))
        reminder = mx_age % r_count
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
                break
        # control reached here means all bins satisfied k-anonymity and we aresetting
        # flag to false and exiting

        if b * r_count + i >= mx_age:
            for b in range(num_bins):
                temp = 0
                eff_rows = r_count
                if b == (num_bins - 1):
                    eff_rows += reminder
                for i in range(1, eff_rows + 1):
                    temp += dataframe["Count"][b * r_count + i]
                # print("Bin index : ", b, "Count : ", temp)
            flag = 0
    return r_count

# function to implement DP
# query: count of users testing positive per PIN Code
# neighbouring dataset: add or remove a user from original dataset
# sensitivity: 1/T

def query_building(dataframe, config):
    output_attribute = config["dp_output_attribute"]
    aggregation_attribute = config["dp_aggregate_attribute"]
    dp_query = config["dp_query"]
    T = len(dataframe)
    # conditional for config options
    if dp_query == "histogram":
        # building custom query for "positivity ratio per PIN code" for count query
        dataframe = dataframe.groupby(aggregation_attribute)[[output_attribute]].agg(
            lambda x: (x == "Positive").sum()
        )
        # dataframe[output_attribute] = dataframe['Test Result']/T
        dataframe[output_attribute] = dataframe["Test Result"]
        # dataframe.drop(output_attribute, axis = 1, inplace = True)
        return dataframe, T, None
    elif dp_query == "mean":
        # building custom query for "time to negative per gender" for mean query
        female_num = dataframe["Gender"].tolist().count("Female")
        male_num = dataframe["Gender"].tolist().count("Male")
        other_num = dataframe["Gender"].tolist().count("Other")
        bin_nums = [female_num, male_num, other_num]
        dataframe = dataframe.groupby(aggregation_attribute)[[output_attribute]].agg(
            "mean"
        )
        return dataframe, T, bin_nums

def medicalDifferentialPrivacy(dataframeAccumulate, configFile, timeRange = 1):
    dpConfig = configFile['differential_privacy']
    count = dataframeAccumulate['query_output'].sum()
    epsilon = dpConfig["dp_epsilon"]
    if dpConfig["dp_query"] == "count":
        sensitivity = 1
        # no epsilon vector is generated
        b = sensitivity/epsilon
        noise = np.random.laplace(0,b,len(dataframeAccumulate))
        privateAggregateDataframe = dataframeAccumulate.copy()
        privateAggregateDataframe["noisy_output"] = privateAggregateDataframe["query_output"] + noise
        return privateAggregateDataframe
    # elif dpConfig["dp_query"] == "mean":
    #     aggregation_attribute = dpConfig["dp_aggregate_attribute"]
    #     # for category in dataframeAccumulate[aggregation_attribute]:
    #         # 
    #     privateAggregateDataframe = dataframeAccumulate.copy()
    #     privateAggregateDataframe["noisy_output"] = privateAggregateDataframe["query_output"] + noise
    #     return privateAggregateDataframe
def differential_privacy(data, config):
    output_attribute = config["dp_output_attribute"]
    aggregation_attribute = config["dp_aggregate_attribute"]
    dp_query = config["dp_query"]
    dataframe, T, bin_nums = query_building(data, config)  # type: ignore
    # print(bin_nums)
    eps_step = config["dp_epsilon_step"]
    # eps_array = np.arange(0.1,10,eps_step)
    eps_array = np.logspace(-3, 2, 50)
    # print(len(dataframe))
    # computing sensitivity for each query
    if dp_query == "histogram":
        sensitivity = 1
        array_of_df = []
        for epsilon in eps_array:
            df_array = dataframe.copy()
            # print('Length of df_array')
            # len(df_array)
            b = sensitivity / epsilon
            noise = np.random.laplace(0, b, len(df_array))
            # print(noise)
            df_array["epsilon"] = epsilon
            # replace with query attribute + noisy
            df_array[f"Noisy {output_attribute}"] = df_array[output_attribute] + noise
            df_array[f"Noisy {output_attribute}"].clip(0, np.inf, inplace=True)
            df_array[f"Noisy {output_attribute}"] = df_array[
                f"Noisy {output_attribute}"
            ].round(4)
            # df_array.drop(columns = output_attribute, inplace = True)
            # print(df_array)
            array_of_df.append(df_array)
        return array_of_df
    elif dp_query == "mean":
        sensitivity_female = 28 / bin_nums[0]  # type: ignore
        sensitivity_male = 28 / bin_nums[1]  # type: ignore
        sensitivity_other = 28 / bin_nums[2]  # type: ignore
        sensitivity = [sensitivity_female, sensitivity_male, sensitivity_other]
        # print(sensitivity)
        array_of_df = []
        for epsilon in eps_array:
            df_array = dataframe.copy()
            noise_array = []
            for sens in sensitivity:
                b = sens / epsilon
                noise = np.random.laplace(0, b, 1)
                noise_array.append(noise)
            noise_array = np.array(noise_array)
            noise_array = noise_array.flatten()
            df_array["epsilon"] = epsilon
            # replace with query attribute + noisy
            # print(df_array[output_attribute])
            df_array[f"Noisy {output_attribute}"] = (
                df_array[output_attribute] + noise_array
            )
            # df_array[f"Noisy {output_attribute}"].clip(0, np.inf, inplace = True)
            df_array[f"Noisy {output_attribute}"] = df_array[
                f"Noisy {output_attribute}"
            ].round(4)
            array_of_df.append(df_array)
        return array_of_df


def output_handler(dataframe_list, config):
    output_attribute = config["dp_output_attribute"]
    aggregation_attribute = config["dp_aggregate_attribute"]
    combined_df = pd.concat(dataframe_list, axis=0)
    combined_df = (
        combined_df.groupby(["epsilon", aggregation_attribute])
        .agg({f"Noisy {output_attribute}": list, output_attribute: list})
        .reset_index()
    )
    data_dict = combined_df.to_dict(orient="records")
    # Group the data by 'epsilon' and create dictionaries with the desired structure
    grouped_data = {}
    for entry in data_dict:
        epsilon_value = entry["epsilon"]
        if epsilon_value not in grouped_data:
            grouped_data[epsilon_value] = []
        grouped_data[epsilon_value].append(
            {
                aggregation_attribute: entry[aggregation_attribute],
                f"Noisy {output_attribute}": entry[f"Noisy {output_attribute}"],
                output_attribute: entry[output_attribute],
            }
        )

    # Convert grouped data to a list of dictionaries
    result_list = [
        {"epsilon": key, "data": value} for key, value in grouped_data.items()
    ]

    # Convert list of dictionaries to JSON format
    json_data = json.dumps(result_list, indent=4)

    # # Writing JSON data to a file
    with open("nestedEpsTestOutputHisto.json", "w") as json_file:
        json_file.write(json_data)
        print("Output File Generated")
    return json_data
