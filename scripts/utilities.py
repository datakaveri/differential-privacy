# import statements
from calendar import c
import pandas as pd
import numpy as np
import json
import hashlib
import matplotlib.pyplot as plt
import logging
###########################

# select logging level
logging.basicConfig(level = logging.INFO)

# for testing
medicalFileList = ["data/syntheticMedicalChunks/medical_data_split_file_0.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_1.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_2.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_3.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_4.json"
                    ]

# for testing

# spatioTemporalFileList = ['data/spatioTemporalChunks/split_file_0.json',
#             'data/spatioTemporalChunks/split_file_1.json',
#             'data/spatioTemporalChunks/split_file_2.json',
#             'data/spatioTemporalChunks/split_file_3.json',
#             'data/spatioTemporalChunks/split_file_4.json',
#             'data/spatioTemporalChunks/split_file_5.json',
#             'data/spatioTemporalChunks/split_file_6.json',
#             'data/spatioTemporalChunks/split_file_7.json',
#             'data/spatioTemporalChunks/split_file_8.json',
#             'data/spatioTemporalChunks/split_file_9.json'
#             ]

spatioTemporalFileList = ['data/spatioTemporalChunks/split_file_0.json',
            'data/spatioTemporalChunks/split_file_1.json',
            'data/spatioTemporalChunks/split_file_2.json',
            'data/spatioTemporalChunks/split_file_3.json',
            'data/spatioTemporalChunks/split_file_4.json',
]

# function definitions
##################################
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

def user_input_handler(config):
    """
    Prompt user to select dataset (medical or spatiotemporal) and processing options (suppression/pseudonymization, k-anonymization, or differential privacy) and update config accordingly.
    """
    print("Select dataset:")
    print("1. Synthetic Medical Data")
    print("2. Real-World SpatioTemporal ITMS Data")

    dataset_choice = input("Enter choice (1 or 2): ")

    dataset_choice = dataset_choice.strip()
    while dataset_choice not in ['1', '2']:
        print("Invalid choice. Please enter '1' or '2':")
        dataset_choice = input("Enter choice (1 or 2): ")
        dataset_choice = dataset_choice.strip()

    if dataset_choice == '1':
        config = config["medical"] # for testing only
        dataset = "medical"
        fileList = medicalFileList
        print("Select processing options:")
        print("1. Suppression")
        print("2. Pseudonymization")
        print("3. K-anonymization")
        print("4. Differential Privacy")

        processing_choice = input("Enter choice (1, 2, 3 or 4): ")

        processing_choice = processing_choice.strip()
        while processing_choice not in ['1', '2', '3', '4']:
            print("Invalid choice. Please enter '1', '2', '3' or '4':")
            processing_choice = input("Enter choice (1, 2, 3 or 4): ")
            processing_choice = processing_choice.strip()

        if processing_choice == '1':
            config = config["suppress"]
            config = {"suppress": config}
            # print(config)
        elif processing_choice == '2':
            config = config["pseudonymize"]
            config = {"pseudonymize": config}
        elif processing_choice == '3':
            config = config["k_anonymize"]
            config = {"k_anonymize": config}
            # print(config)
        elif processing_choice == '4':
            print("Select query:")
            print("1. Count")
            print("2. Mean")

            query_choice = input("Enter choice (1 or 2): ")

            query_choice = query_choice.strip()
            while query_choice not in ['1', '2']:
                print("Invalid choice. Please enter '1' or '2':")
                query_choice = input("Enter choice (1 or 2): ")
                query_choice = query_choice.strip()

            if query_choice == '1':
                config["differential_privacy"]["dp_query"] = "count"
                config["differential_privacy"]["dp_output_attribute"] = "Test Result"
                config["differential_privacy"]["dp_aggregate_attribute"] = "PIN Code"

            elif query_choice == '2':
                config["differential_privacy"]["dp_query"] = "mean"
                config["differential_privacy"]["dp_output_attribute"] = "Days to Negative"
                config["differential_privacy"]["dp_aggregate_attribute"] = "Gender"

    elif dataset_choice == '2':
        config = config["spatioTemporal"] # for testing only 
        dataset = "spatioTemporal"
        fileList = spatioTemporalFileList
        print("Select processing options:")
        print("1. Suppression")
        print("2. Pseudonymization")
        print("3. Differential Privacy")

        processing_choice = input("Enter choice (1, 2 or 3): ")

        processing_choice = processing_choice.strip()
        while processing_choice not in ['1', '2', '3']:
            print("Invalid choice. Please enter '1', '2', or '3':")
            processing_choice = input("Enter choice (1, 2 or 3): ")
            processing_choice = processing_choice.strip()

        if processing_choice == '1':
            config = config["suppress"]
            config = {"suppress": config}
        # print(config)
        elif processing_choice == '2':
            config = config["pseudonymize"]
            config = {"pseudonymize": config}
        elif processing_choice == '3':
            print("Select query:")
            print("1. Count")
            print("2. Mean")

            query_choice = input("Enter choice (1 or 2): ")

            query_choice = query_choice.strip()
            while query_choice not in ['1', '2']:
                print("Invalid choice. Please enter '1' or '2':")
                query_choice = input("Enter choice (1 or 2): ")
                query_choice = query_choice.strip()

            if query_choice == '1':
                config["differential_privacy"]["dp_query"] = "count"

            elif query_choice == '2':
                config["differential_privacy"]["dp_query"] = "mean"

    return config, dataset, fileList

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
    return mean_absolute_error

def post_processing(data, config):
    # TODO: Decide on format of output attribute names
    dpConfig = config["differential_privacy"]
    output_attribute = dpConfig["dp_output_attribute"]
    if dpConfig['dp_query'] == 'mean':
        data[f"Noisy {output_attribute}"] = data[f"Noisy {output_attribute}"].clip(0)
        data[f"Noisy {output_attribute}"] = data[f"Noisy {output_attribute}"].round(3)
    elif dpConfig['dp_query'] == 'count':
        data[f"Noisy {dpConfig['dp_query']}"] = data[f"Noisy {dpConfig['dp_query']}"].clip(0)
        data[f"Noisy {dpConfig['dp_query']}"] = data[f"Noisy {dpConfig['dp_query']}"].round(0)
    return data

# function to handle dataset choice
def dataset_handler(config):
    if config["data_type"] == "medical":
        config = config["medical"] # for testing only
        dataset = "medical"
        fileList = medicalFileList
    elif config["data_type"] == "spatioTemporal":
        config = config["spatioTemporal"] # for testing only 
        dataset = "spatioTemporal"
        fileList = spatioTemporalFileList
    return dataset, config, fileList

def output_handler_k_anon(data, config):
    dataset_name = data.name
    kConfig = config["k_anonymize"]
    data.drop(columns=[config["k_anonymize"]["generalize"]], inplace=True)
    data[f"{kConfig['generalize']} Bin"] = data[f"{kConfig['generalize']} Bin"].astype(str)
    data = data.to_json(orient='index')
    file_name = f'pipelineOutput/medical_k_anon_{dataset_name}'
    with open(f"{file_name}.json", 'w') as outfile:
        outfile.write(data)
    logging.info('k-anonymized data saved to %s', file_name)
    logging.info('k-anonymity level: %s', kConfig["k"])
    return data

def output_handler_spatioTemp_mae(mean_absolute_error, config):
    dpConfig = config["differential_privacy"]
    if dpConfig['dp_query'] == 'mean':
        averaged_mean_absolute_error = np.mean(mean_absolute_error, axis=0) # averaged over all the HATs  
        averaged_mean_absolute_error = averaged_mean_absolute_error.to_json(orient='index')
        mean_absolute_error = mean_absolute_error.set_index('HAT')
        mean_absolute_error = mean_absolute_error.to_json(orient='index')
        file_name = 'pipelineOutput/spatioTemporal_EpsVSMAEperHAT'
        with open(f"{file_name}_{dpConfig['dp_query']}.json", 'w') as outfile:
            outfile.write(mean_absolute_error)
        logging.info('%s query error table saved to %s_%s', dpConfig['dp_query'], file_name, dpConfig['dp_query'])
        file_name = 'pipelineOutput/spatioTemporal_EpsVSMAE'
        with open(f"{file_name}_{dpConfig['dp_query']}.json", 'w') as outfile:
            outfile.write(averaged_mean_absolute_error) 
        logging.info('%s query averaged error table saved to %s_%s', dpConfig['dp_query'], file_name, dpConfig['dp_query'])

    elif dpConfig['dp_query'] == 'count':
        # mean_absolute_error = mean_absolute_error.squeeze(axis=0)
        mean_absolute_error = mean_absolute_error[0]
        mean_absolute_error = mean_absolute_error.to_json(orient='index')
        file_name = 'pipelineOutput/EpsVSMAE'
        with open(f"{file_name}_{dpConfig['dp_query']}.json", 'w') as outfile:
            outfile.write(mean_absolute_error)
        logging.info('%s query error table saved to %s_%s', dpConfig['dp_query'], file_name, dpConfig['dp_query'])
    return 

def output_handler_medical_mae(mean_absolute_error, config):
    dpConfig = config["differential_privacy"]
    if dpConfig["dp_query"] == 'count':
        mean_absolute_error = mean_absolute_error[0]
        mean_absolute_error = mean_absolute_error.to_json(orient='index')
    elif dpConfig["dp_query"] == 'mean':
        mean_absolute_error = mean_absolute_error.to_json(orient='index')
    file_name = 'pipelineOutput/medical_EpsVSMAE'
    with open(f"{file_name}_{dpConfig['dp_query']}.json", 'w') as outfile:
        outfile.write(mean_absolute_error)
    logging.info('%s query error table saved to %s_%s', dpConfig['dp_query'], file_name, dpConfig['dp_query'])
    return

def output_handler_medical_dp_data(data, config):
    dpConfig = config['differential_privacy']
    if dpConfig['dp_query'] == 'count':
        # data = pd.DataFrame(data)
        aggregate_attribute = dpConfig['dp_aggregate_attribute']
        data = data.set_index(aggregate_attribute)
    elif dpConfig['dp_query'] == 'mean':
        aggregate_attribute = dpConfig['dp_aggregate_attribute']
        data = data.set_index(aggregate_attribute)
    data = data.to_json(orient='index')
    file_name = 'pipelineOutput/medical_noisyQueryOutput'
    with open(f"{file_name}_{dpConfig['dp_query']}.json", "w") as outfile:
        outfile.write(data)
    logging.info('%s query output saved to %s_%s', dpConfig['dp_query'], file_name, dpConfig['dp_query'])
    return data

def output_handler_spatioTemp_dp_data(data, config):
    dpConfig = config['differential_privacy']
    data = data[['HAT', 'query_output','noisy_output']]
    data = data.set_index('HAT')
    data = data.to_json(orient='index')
    file_name = 'pipelineOutput/spatioTemporal_noisyQueryOutput'
    with open(f"{file_name}_{dpConfig['dp_query']}.json", "w") as outfile:
        outfile.write(data)
    logging.info('%s query output saved to %s_%s', dpConfig['dp_query'], file_name, dpConfig['dp_query'])
    return data

def oop_handler(config, dataset):
    operations = []
    if "suppress" in config:
        operations.append("suppress")
    if "pseudonymize" in config:
        operations.append("pseudonymize")
    if "k_anonymize" in config:
        operations.append("k_anonymize")
    if "differential_privacy" in config:
        operations.append("dp")
    return operations

def monte_carlo_sim_mae(num_iterations, epsilon_vector, bVector_sum, bVector_count, sum, count ):
    mae_vector = []
    for i in range(len(epsilon_vector)):
        b_sum = bVector_sum[i]
        b_count = bVector_count[i]
        noisy_vector_sum = np.random.laplace(0,b_sum,int(num_iterations)) + sum
        noisy_vector_count = np.random.laplace(0,b_count,int(num_iterations)) + count
        noisy_mean_vector = noisy_vector_sum/noisy_vector_count
        abs_error_vector = abs(noisy_mean_vector - (sum/count))
        abs_error_vector_sum = abs_error_vector.sum()
        mean_absolute_error = abs_error_vector_sum/num_iterations
        mae_vector.append(mean_absolute_error)
    return mae_vector


#################################################################
# DEPRECATED
# function to compute the normalized mae
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

# function to plot/table the mean_normalised_mae against the epsilon vector 
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
