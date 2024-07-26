# import statements
from numpy import mean
import scripts.medicalPipeline as medpipe
import scripts.spatioTemporalPipeline as stpipe
import scripts.utilities as utils
import json
import os

config_file_name = os.path.join('config', os.listdir('config')[0])
config = utils.read_config(config_file_name)
dataset = config["data_type"]
operations = config["operations"]
config = config[dataset]
# checking the dataset order of operations selected
fileList = [file for file in os.popen('ls data/*.json').read().split('\n') if file]
print(fileList)
# dataset = utils.user_input_handler(config)  
# operations = utils.oop_handler(config, dataset)
# dataset, config, fileList = utils.dataset_handler(config)
print(dataset, operations)
print(config)

# selecting appropriate pipeline
if dataset == "medical":

    if "dp" in operations:
        data, mean_absolute_error = medpipe.medicalPipelineDP(config, operations, fileList)
        data = utils.post_processing(data, config)
        formatted_error = utils.output_handler_medical_mae(mean_absolute_error, config)
        formatted_data = utils.output_handler_medical_dp_data(data, config)
        print("dp",data)
        # print(formatted_error)
    if "suppress" in operations:
        data = medpipe.medicalPipelineSuppressPseudonymize(config, operations, fileList)
    if "pseudonymize" in operations:
        data = medpipe.medicalPipelineSuppressPseudonymize(config, operations, fileList)
    if "k_anonymize" in operations:
        k_anonymized_dataset, user_counts = medpipe.medicalPipelineKAnon(data, config, operations, fileList)  
        formatted_dataset = utils.output_handler_k_anon(k_anonymized_dataset, config)
        formatted_user_counts = utils.output_handler_k_anon(user_counts, config)
        # print("optimal bin width: ", optimal_bin_width)

if dataset == "spatioTemporal":

    if "dp" in operations:
        data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
        data = utils.post_processing(data, config)
        mean_absolute_error = utils.mean_absolute_error(bVector)
        formatted_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)
        formatted_data = utils.output_handler_spatioTemp_dp_data(data, config)
        if "suppress" in operations:
            data = stpipe.spatioTemporalPipeline(config, operations, fileList)
        if "pseudonymize" in operations:
            data = stpipe.spatioTemporalPipeline(config, operations, fileList)
