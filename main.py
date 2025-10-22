# import statements
import scripts.Pipeline as pipe
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

# selecting appropriate pipeline

if dataset == "spatioTemporal":
    if config["differential_privacy"]["dp_query"] == 'mean':
        data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
        data = utils.post_processing(data, config)
        mean_absolute_error = utils.mean_absolute_error(bVector)
        formatted_error, formatted_averaged_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)          
        formatted_data = utils.output_handler_spatioTemp_dp_data(data, config)
        concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error_per_hat = formatted_error, epsilon_vs_averaged_error = formatted_averaged_error)
    if config["differential_privacy"]["dp_query"] == 'count':
        data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
        data = utils.post_processing(data, config)
        mean_absolute_error = utils.mean_absolute_error(bVector)
        formatted_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)
        formatted_data = utils.output_handler_spatioTemp_dp_data(data, config)
        concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error = formatted_error)
else:
    data, mean_absolute_error, noisy_query_output_for_epsilon_vector = pipe.generalPipelineDP(config, operations, fileList)
    data = utils.post_processing(data, config)
    formatted_error = utils.output_handler_general_mae(mean_absolute_error, config)
    formatted_data = utils.output_handler_general_dp_data(data, config)
    formatted_noise_vector = utils.output_handler_general_noise_vector(noisy_query_output_for_epsilon_vector)
    concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error = formatted_error, noise_vector = formatted_noise_vector)