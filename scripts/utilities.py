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

# drop duplicates
def deduplicate(dataframe):
    # df.drop_duplicates does not work on dataframes containing lists
    # identifying attributes with lists 
    list_attributes = [col for col in dataframe.columns if dataframe[col].apply(lambda x: isinstance(x, list)).any()]
    attributes_without_lists = [col for col in dataframe.columns if col not in list_attributes]
    # dropping all attributes in subset of columns without lists
    dataframe = dataframe.drop_duplicates(
        subset = attributes_without_lists, 
        inplace = False, ignore_index = True)
    return dataframe

# suppress
def suppress(dataframe, config):
    attributes_to_suppress = config['suppress']
    dataframe.drop(columns = attributes_to_suppress, inplace = True)
    # print(dataframe.info())
    return dataframe

# pseudonymize
def pseudonymize(dataframe, config):
    attribute_to_pseudonymize = config['pseudonymize']
    dataframe['UID'] = dataframe[attribute_to_pseudonymize[0]] + dataframe[attribute_to_pseudonymize[1]]
    dataframe['Hashed Value'] = dataframe['UID'].apply(lambda x:hashlib.sha256(x.encode()).hexdigest())
    dataframe.drop(columns=['UID'] + attribute_to_pseudonymize, inplace=True)
    return dataframe

###########################
# function to handle order of operations and select config
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