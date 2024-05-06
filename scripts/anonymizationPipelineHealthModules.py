# import statements
import pandas as pd
import numpy as np
import json
import hashlib

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
    attributes_to_suppress = config['suppress']
    dataframe.drop(columns = attributes_to_suppress, inplace = True)
    print(dataframe.info())
    return dataframe

# pseudonymize
def pseudonymize(dataframe, config):
    attribute_to_pseudonymize = config['pseudonymize']
    dataframe['UID'] = dataframe[attribute_to_pseudonymize[0]] + dataframe[attribute_to_pseudonymize[1]]
    dataframe['Hashed Value'] = dataframe['UID'].apply(lambda x:hashlib.sha256(x.encode()).hexdigest())
    dataframe.drop(columns=['UID'] + attribute_to_pseudonymize, inplace=True)
    return dataframe

###########################
# functions for k-anonymity

# function to bin the ages
def generalize(dataframe, config, bins):
    attribute_to_generalize = config["generalize"]
    dataframe['Age Bin'] = pd.cut(dataframe[attribute_to_generalize], bins, ordered = True)
    return dataframe

# function to check if a bin violates k-anonymity
def violation_checker(k, data):
    age_value_counts = data['Age Bin'].value_counts()
    violating_bins = age_value_counts[(age_value_counts < k) & (age_value_counts > 0)].index
    # print(violating_bins)
    if len(violating_bins) > 0:
        return True
    else:
        return False

# function to k-anonymize
def k_anonymize(dataframe, config):
    attribute_to_generalize = config["generalize"]
    k = config["k"]
    min_bin_value = config["min_bin_value"]
    max_bin_value = config["max_bin_value"]
    age_bins = np.arange(min_bin_value, max_bin_value, 1)
    dataframe = generalize(dataframe, config, np.arange(min_bin_value,max_bin_value,1))
    # If ANY bin violates k-anonymity, increment the size of ALL bins
    for i in range(1, (len(age_bins) - 1)):
        while violation_checker(k, dataframe) == True:
            dataframe = generalize(dataframe, config, np.arange(min_bin_value,max_bin_value,i))
            age_value_counts = dataframe['Age Bin'].value_counts()
            i+=1
    dataframe.drop(columns = attribute_to_generalize, inplace = True)
    age_value_counts = age_value_counts[age_value_counts != 0]
    return dataframe, age_value_counts

###########################
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
    if dp_query == 'histogram':
        # building custom query for "positivity ratio per PIN code" for count query
        dataframe = dataframe.groupby(aggregation_attribute)[[output_attribute]].agg(lambda x: (x == 'Positive').sum())
        # dataframe[output_attribute] = dataframe['Test Result']/T
        dataframe[output_attribute] = dataframe['Test Result']
        # dataframe.drop(output_attribute, axis = 1, inplace = True)
        return dataframe, T, None
    elif dp_query == 'mean':
        # building custom query for "time to negative per gender" for mean query
        female_num = dataframe['Gender'].tolist().count('Female')
        male_num = dataframe['Gender'].tolist().count('Male')
        other_num = dataframe['Gender'].tolist().count('Other')
        bin_nums = [female_num, male_num, other_num]
        dataframe = dataframe.groupby(aggregation_attribute)[[output_attribute]].agg('mean')
        return dataframe, T, bin_nums

def differential_privacy(data, config):
    output_attribute = config["dp_output_attribute"]
    aggregation_attribute = config["dp_aggregate_attribute"]
    dp_query = config["dp_query"]
    dataframe, T, bin_nums = query_building(data, config)
    # print(bin_nums)
    eps_step = config["dp_epsilon_step"]
    # eps_array = np.arange(0.1,10,eps_step)  
    eps_array = np.logspace(-3, 2, 50)
    # print(len(dataframe))
    # computing sensitivity for each query
    if dp_query == 'histogram':   
        sensitivity = 1
        array_of_df = []
        for epsilon in eps_array:
            df_array = dataframe.copy()
            # print('Length of df_array')
            # len(df_array)
            b = sensitivity/epsilon
            noise = np.random.laplace(0, b, len(df_array))
            # print(noise)
            df_array["epsilon"] = epsilon
            #replace with query attribute + noisy
            df_array[f"Noisy {output_attribute}"] = df_array[output_attribute] + noise
            df_array[f"Noisy {output_attribute}"].clip(0, np.inf, inplace = True)
            df_array[f"Noisy {output_attribute}"] = df_array[f"Noisy {output_attribute}"].round(4)
            # df_array.drop(columns = output_attribute, inplace = True)
            # print(df_array)
            array_of_df.append(df_array)
        return array_of_df
    elif dp_query == 'mean':
        sensitivity_female = 28/bin_nums[0]
        sensitivity_male = 28/bin_nums[1]
        sensitivity_other = 28/bin_nums[2]
        sensitivity = [sensitivity_female, sensitivity_male, sensitivity_other]
        # print(sensitivity)
        array_of_df = []
        for epsilon in eps_array:
            df_array = dataframe.copy()
            noise_array = []
            for sens in sensitivity:
                b = sens/epsilon
                noise = np.random.laplace(0,b,1)
                noise_array.append(noise)
            noise_array = np.array(noise_array)
            noise_array = noise_array.flatten()
            df_array["epsilon"] = epsilon
            # replace with query attribute + noisy
            # print(df_array[output_attribute])
            df_array[f"Noisy {output_attribute}"] = df_array[output_attribute] + noise_array
            # df_array[f"Noisy {output_attribute}"].clip(0, np.inf, inplace = True)
            df_array[f"Noisy {output_attribute}"] = df_array[f"Noisy {output_attribute}"].round(4)
            array_of_df.append(df_array)
        return array_of_df

def output_handler(dataframe_list, config):
    output_attribute = config["dp_output_attribute"]
    aggregation_attribute = config["dp_aggregate_attribute"]
    combined_df = pd.concat(dataframe_list, axis = 0)
    combined_df =  combined_df.groupby(["epsilon",aggregation_attribute]).agg({f"Noisy {output_attribute}": list, output_attribute: list}).reset_index()
    data_dict = combined_df.to_dict(orient="records")
    # Group the data by 'epsilon' and create dictionaries with the desired structure
    grouped_data = {}
    for entry in data_dict:
        epsilon_value = entry['epsilon']
        if epsilon_value not in grouped_data:
            grouped_data[epsilon_value] = []
        grouped_data[epsilon_value].append({
            aggregation_attribute: entry[aggregation_attribute],
            f"Noisy {output_attribute}": entry[f"Noisy {output_attribute}"],
            output_attribute: entry[output_attribute]
        })

    # Convert grouped data to a list of dictionaries
    result_list = [{'epsilon': key, 'data': value} for key, value in grouped_data.items()]

    # Convert list of dictionaries to JSON format
    json_data = json.dumps(result_list, indent=4)

    # # Writing JSON data to a file
    with open('nestedEpsTestOutputHisto.json', 'w') as json_file:
        json_file.write(json_data)
        print("Output File Generated")
    return json_data

###########################
# function to handle order of operations
def oop_handler(config):
    operations = []
    if "suppress" in config:
        operations.append("suppress")
    if "pseudonymize" in config:
        operations.append("pseudonymize")
    if "generalize" in config:
        operations.append("k_anonymize")
    if "dp_query" in config:
        operations.append("dp")
    return operations