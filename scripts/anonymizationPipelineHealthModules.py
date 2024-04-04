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
    dataframe['Hashed Value'] = dataframe[attribute_to_pseudonymize].apply(lambda x:hashlib.sha256(x.encode()).hexdigest())
    dataframe.drop(columns = attribute_to_pseudonymize, inplace = True)
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
            print(age_value_counts) 
            i+=1
    dataframe.drop(columns = attribute_to_generalize, inplace = True)
    return dataframe

###########################
# function to implement DP
    # query: number of users for a diagnosis
    # neighbouring dataset: add or remove a user from original dataset
    # sensitivity: 1

def query_building(dataframe, config):
    query_column = config["dp_query"]
    aggregation_column = config["dp_aggregation"]
    T = len(dataframe)
    positive_count = dataframe.groupby('PIN Code')[['Test Result']].agg(lambda x: (x == 'Positive').sum())    # dataframe = dataframe.groupby(aggregation_column)
    dataframe = positive_count
    dataframe["Positivity Ratio"] = dataframe['Test Result']/T
    # dataframe.rename(columns={"Test Result":"Positive Count"}, inplace = True)
    dataframe.drop("Test Result", axis = 1, inplace = True)
    print(dataframe)
    return dataframe, T

def differential_privacy(dataframe, config):
    dataframe, T = query_building(dataframe, config)
    epsilon = config["dp_epsilon"]
    sensitivity = 1/T
    b = sensitivity/epsilon
    noise = np.random.laplace(0, b, len(dataframe))
    print(noise)
    dataframe["Noisy Positivity Ratio"] = dataframe["Positivity Ratio"] + noise
    dataframe["Noisy Positivity Ratio"].clip(0, np.inf, inplace = True)
    dataframe["Noisy Positivity Ratio"] = dataframe["Noisy Positivity Ratio"].round(4)
    dataframe.drop(columns = "Positivity Ratio", inplace = True)
    return dataframe

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
    if "dp_aggregation" in config:
        operations.append("dp")
    return operations