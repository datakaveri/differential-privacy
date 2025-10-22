# import statements
import scripts.medicalPipeline as medpipe
import scripts.spatioTemporalPipeline as stpipe
import scripts.utilities as utils
import json, os, requests
import logging
from iudx_dp_validations import *

def main_process(config):
    # checking the dataset order of operations selected
    try:
        dataset = config["data_type"]
        operations = config["operations"]
        config = config[dataset]

        #validate config
        validate_dp_conf_obj = ValidateDPConfig(config)
        validate_dp_conf_obj.validate_dp_config()
        
        # checking the dataset order of operations selected
        fileList = []
        if dataset == "spatioTemporal":
            fileList = [file for file in os.popen('ls data/spatioTemporalChunks/*.json').read().split('\n') if file]
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
            fileList = [file for file in os.popen('ls data/chunks/*.json').read().split('\n') if file]       
            data, mean_absolute_error, noisy_query_output_for_epsilon_vector = medpipe.medicalPipelineDP(config, operations, fileList)
            data = utils.post_processing(data, config)
            # logging.info("Finished Post Processing")
            formatted_error = utils.output_handler_medical_mae(mean_absolute_error, config)
            # logging.info("Finished utput_handler_medical_mae")
            formatted_data = utils.output_handler_medical_dp_data(data, config)
            # logging.info("Finished output_handler_medical_dp_data")
            formatted_noise_vector = utils.output_handler_medical_noise_vector(noisy_query_output_for_epsilon_vector)
            # logging.info("Finished output_handler_medical_noise_vector")
            concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error = formatted_error, noise_vector = formatted_noise_vector)
            # logging.info("Finished output_concatenator")

        concat_output['status'] = "success"
        concat_output['status_code'] = "0000"
    except Exception as e:
        if isinstance(e, CustomValueError):
            concat_output = {
            'status': 'failed',
            'status_code': e.code,
            'error_message': e.message
        }
            print(f"Caught a CustomError: {e}")
        else:
            concat_output = {
            'status': 'failed',
            'status_code': "1111",
            'error_message': type(e).__name__
        }
            print(f"Error: {e}")            
    return concat_output

