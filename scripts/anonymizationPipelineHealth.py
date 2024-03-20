# import statements
import pandas as pd
import numpy as np
import json
import hashlib

# function definitions

# reading config
def read_config(configFile):
    with open(configFile, "r") as cfile:
        config = json.load(cfile)
    return config

# reading data
def read_data(dataFile):
    with open(dataFile, "r") as dfile:
        data = json.load(dfile)
        dataframe = pd.json(data)
    return dataframe

# suppress
def suppress(dataframe, config):
    attributes_to_suppress = config['suppress']
    dataframe.drop(columns = suppress, inplace = True)
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
def generalize_age(dataframe, bins):
    dataframe['Age Bin'], retbins = pd.cut(dataframe['Age'], bins, retbins = True, ordered = True)
    return dataframe, retbins

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
def k_anonymize(dataframe, config, age_bins):
    k = config["k"]
    min_bin_value = config["min_bin_value"]
    max_bin_value = config["max_bin_value"]
    # age_bins = np.arange(min_bin_value, max_bin_value, 1)
    # If ANY bin violates k-anonymity, increment the size of ALL bins
    for i in range(1, (len(age_bins) - 1)):
        while violation_checker(k, data, bins) == True:
            data, bins = generalize_age(data, np.arange(0,101,i))
            age_value_counts = data['Age Bin'].value_counts()
            print(age_value_counts) 
            i+=1
    return dataframe

###########################
# function to implement DP
    # query: number of users for a diagnosis
    # neighbouring dataset: add or remove a user from original dataset
    # sensitivity: 1

def query_building(dataframe, config):
    query_column = config["dp_query"]
    aggregation_column = config["dp_aggregation"]
    dataframe = dataframe.groupby(query_column).agg({aggregation_column:"count"})
    dataframe.rename(columns={aggregation_column:"Count"}, inplace = True)
    return dataframe

def differential_privacy(dataframe, config):
    epsilon = config["dp_epsilon"]
    sensitivity = 1
    b = sensitivity/epsilon
    noise = np.random.laplace(0, b, len(dataframe))
    dataframe = dataframe["Count"] + noise
    dataframe.rename(columns={"Count":"Noisy Count"})
    return dataframe