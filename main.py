# import statements
import scripts.medicalPipeline as medpipe
import scripts.spatioTemporalPipeline as stpipe
import scripts.utilities as utils
import json
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

# necessary file reads
config_file_name = "testConfigs/spatioDP.json"
config = utils.read_config(config_file_name)

# prompt user to select query
print("Select query:")
print("1. Count")
print("2. Mean")

choice = input("Enter choice (1 or 2): ")

choice = choice.strip()
while choice not in ['1', '2']:
    print("Invalid choice. Please enter '1' or '2':")
    choice = input("Enter choice (1 or 2): ")
    choice = choice.strip()

if choice == '1':
    config["spatioTemporal"]["differential_privacy"]["dp_query"] = "count"
elif choice == '2':
    config["spatioTemporal"]["differential_privacy"]["dp_query"] = "mean"
# data = utils.read_data(config["data_file"])

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

# checking the dataset order of operations selected
operations = utils.oop_handler(config)
dataset, config, fileList = dataset_handler(config)
# print(dataset, operations)

# selecting appropriate pipeline
if dataset == "medical":
    data = medpipe.medicalPipeline(config, operations, fileList)

if dataset == "spatioTemporal":

    if "dp" in operations:
        data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
        data = utils.post_processing(data, config)
        mean_absolute_error = utils.mean_absolute_error(bVector)
        formatted_error = utils.output_handler_mae(mean_absolute_error, config)
        formatted_data = utils.output_handler_spatioTemp_data(data, config)

    elif ("suppress" or "pseudonymize") in operations and ("dp") not in operations:
        data = stpipe.spatioTemporalPipeline(config, operations, fileList)
# print(data)

# TODO: Add output format handling (json dumps)
