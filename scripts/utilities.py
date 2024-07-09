# import statements
import pandas as pd
import numpy as np
import json
import hashlib
import matplotlib.pyplot as plt
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

def mean_absolute_error(bVector):
    mean_absolute_error = bVector
    mean_absolute_error = mean_absolute_error.set_index('HAT')
    averaged_mean_absolute_error = np.mean(mean_absolute_error, axis=0) # averaged over all the HATs  
    return mean_absolute_error, averaged_mean_absolute_error


# function to plot/table the mean_normalised_mae against the epsilon vector 

def output_handler_mae(mean_absolute_error, averaged_mean_absolute_error):
    mean_absolute_error = mean_absolute_error.to_json(orient='index')
    with open('pipelineOutput/EpsVSMAEperHAT.json', 'w') as outfile:
        outfile.write(mean_absolute_error)
    
    averaged_mean_absolute_error = averaged_mean_absolute_error.to_json(orient='index')
    with open('pipelineOutput/EpsVSMAE.json', 'w') as outfile:
        outfile.write(averaged_mean_absolute_error)
    return 

def plot_normalised_mae(mean_normalised_mae, config):
    plt.close()
    chosen_epsilon = config['differential_privacy']['dp_epsilon']
    epsilonVector = np.arange(0.1,5,chosen_epsilon)
    # epsilonVector = np.logspace(-5, 0, 1000)
    plt.plot(epsilonVector, mean_normalised_mae)
    plt.plot(chosen_epsilon, mean_normalised_mae[int((chosen_epsilon-0.1)/chosen_epsilon)], 'x', markersize=5, markerfacecolor='red')
    plt.xlabel('Epsilon')
    plt.ylabel('Normalised Mean Absolute Error')
    plt.title('Normalised Mean Absolute Error vs Epsilon')
    plt.grid(True)
    # plt.show()
    
    # create a json file with epsilonVector and mean_normalised_mae
    # output = []
    # for e, m in zip(epsilonVector, mean_normalised_mae):
    #     output.append({'epsilon': e, 'mean_normalised_mae': m})
    # with open('pipelineOutput/epsilonVector_mean_normalised_mae.json', 'w') as f:
    #     json.dump(output, f)
    return

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

# TODO: Rewrite output_handler
def output_handler(data):
    data['timeSlot'] = data['HAT'].apply(lambda x: x[:2])
    formatted_data = {}
    for idx, row in data.iterrows():
        hat_string = row['HAT']
        hat_time = hat_string[:2]
        hat_id = hat_string[2:]
        if hat_id not in formatted_data:
            formatted_data[hat_id] = {}
        if hat_time not in formatted_data[hat_id]:
            formatted_data[hat_id][hat_time] = []
        formatted_data[hat_id][hat_time].append({
            'HAT': hat_id,
            'query_output': row['query_output'],
            'noisy_output': row['noisy_output']
        })
    with open("pipelineOutput/testOutput_formatUpdated.json", "w") as f:
        json.dump(output_handler(data), f, indent=4)
    data.to_json("pipelineOutput/testOutput_formatTest.json")
    return formatted_data


def normalized_mean_absolute_error(dataframeAccumulate, bVector):
    
    true_values = dataframeAccumulate["query_output"]
    normalised_mae = []
    mean_normalised_mae = []

    # iterating over the number of categories = len(sensitivity)
    # if len(sensitivity) = 1, i.e. there is one category
    if bVector.ndim == 1:
        # the expected value of mae for laplace is b, dividing by true value to normalize
        for true_value in true_values:
            normalised_mae.append(bVector/true_value)
        normalised_mae = np.array(normalised_mae)
    else:
        # the expected value of mae for laplace is b, dividing by true value to normalize
        normalised_mae = [bVector[i,:]/true_values[i] for i in range(len(true_values))]
        normalised_mae = np.array(normalised_mae)
       
    # num_vectors is equal to the length of the epsilon vector
    num_vectors = normalised_mae.shape[1]
    mean_normalised_mae = np.zeros(num_vectors)

    print(num_vectors)
    # iterating over the number of epsilons
    for i in range((num_vectors)):
        # taking the means of the columns of the vector
        if i == 1:
            print("this is the sum of the 1st column: ", np.nansum(normalised_mae[:,i]))
            print(normalised_mae[:,1])
        mean_normalised_mae[i] = np.mean(normalised_mae[:, i]) 

    ######################################################################
    # print statements for testing

    print("dataframeAccumulate.columns:", dataframeAccumulate.columns)
    print("bVector.shape", bVector.shape)
    print("true_values.shape:", true_values.shape)
    print("normalised MAE: ", normalised_mae)
    print("shape MAE: ", (normalised_mae.shape))
    print(mean_normalised_mae, len(mean_normalised_mae))

    return mean_normalised_mae



