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
    dataType = config["data_type"]
    if dataType == "medical":
        config = config["medical"]
    elif dataType == "spatioTemporal":
        config = config["spatioTemporal"]
    if "suppress" in config:
        operations.append("suppress")
    if "pseudonymize" in config:
        operations.append("pseudonymize")
    if "generalize" in config:
        operations.append("k_anonymize")
    if "differential_privacy" in config:
        operations.append("dp")
    return operations

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
