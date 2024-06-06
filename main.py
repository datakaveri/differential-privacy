# import statements
import scripts.medicalPipeline as medpipe
import scripts.spatioTemporalPipeline as stpipe
# import scripts.medicalModules as medmod
# import scripts.spatioTemporalModules as stmod
# import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils

# for testing
medicalFileList = ["../data/syntheticMedicalChunks/medical_data_split_file_0.json",
                    "../data/syntheticMedicalChunks/medical_data_split_file_1.json",
                    "../data/syntheticMedicalChunks/medical_data_split_file_2.json",
                    "../data/syntheticMedicalChunks/medical_data_split_file_3.json",
                    "../data/syntheticMedicalChunks/medical_data_split_file_4.json"
                    ]

# for testing
spatioTemporalFileList = ['../data/spatioTemporalChunks/split_file_0.json',
            '../data/spatioTemporalChunks/split_file_1.json',
            '../data/spatioTemporalChunks/split_file_2.json',
            '../data/spatioTemporalChunks/split_file_3.json',
            '../data/spatioTemporalChunks/split_file_4.json',
            '../data/spatioTemporalChunks/split_file_5.json',
            '../data/spatioTemporalChunks/split_file_6.json',
            '../data/spatioTemporalChunks/split_file_7.json',
            '../data/spatioTemporalChunks/split_file_8.json',
            '../data/spatioTemporalChunks/split_file_9.json'
            ]

# necessary file reads
config = utils.read_config("../config/spatioDP.json")
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
print(dataset, operations)

# selecting appropriate pipeline
if dataset == "medical":
    data = medpipe.medicalPipeline(config, operations, fileList)
if dataset == "spatioTemporal":
    data = stpipe.spatioTemporalPipeline(config, operations, fileList)

print(data)

# TODO: Add output format handling (json dumps)
# TODO: Add RRMSE vs Epsilon computation and output
# mods.output_handler(data, config)

