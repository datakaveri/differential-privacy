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