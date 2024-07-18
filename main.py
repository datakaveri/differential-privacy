# import statements
from numpy import mean
import scripts.medicalPipeline as medpipe
import scripts.spatioTemporalPipeline as stpipe
import scripts.utilities as utils
import json

config_file_name = "config/pipelineConfig.json"
config = utils.read_config(config_file_name)

# checking the dataset order of operations selected
config, dataset, fileList = utils.user_input_handler(config)  
operations = utils.oop_handler(config, dataset)
# dataset, config, fileList = utils.dataset_handler(config)
print(dataset, operations)
print(config)

# selecting appropriate pipeline
if dataset == "medical":

    if "dp" in operations:
        data, bVector = medpipe.medicalPipelineDP(config, operations, fileList)
        mean_absolute_error = utils.mean_absolute_error(bVector)
        formatted_error = utils.output_handler_medical_mae(mean_absolute_error, config)
        formatted_data = utils.output_handler_medical_data(data, config)
        print("dp",data)
        # print(formatted_error)
    elif "k_anonymize" in operations:
        print("reached k-anon")
        optimal_bin_width = medpipe.medicalPipelineKAnon(config, operations, fileList)  
        print("optimal bin width: ", optimal_bin_width)
    elif ("suppress") in operations:
        data = medpipe.medicalPipelineSuppressPseudonymize(config, operations, fileList)
        print(data)
    elif ("pseudonymize") in operations:
        data = medpipe.medicalPipelineSuppressPseudonymize(config, operations, fileList)
        print(data)

if dataset == "spatioTemporal":

    if "dp" in operations:
        # Prompt user for query
        data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
        data = utils.post_processing(data, config)
        mean_absolute_error = utils.mean_absolute_error(bVector)
        formatted_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)
        formatted_data = utils.output_handler_spatioTemp_data(data, config)

    elif ("suppress") in operations:
        data = stpipe.spatioTemporalPipeline(config, operations, fileList)
        print(data)
    elif ("pseudonymize") in operations:
        data = stpipe.spatioTemporalPipeline(config, operations, fileList)
        print(data)


# TODO: Add output format handling (json dumps)
