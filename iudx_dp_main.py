# import statements
import scripts.medicalPipeline as medpipe
import scripts.spatioTemporalPipeline as stpipe
import scripts.utilities as utils
import json, os

def main_process(config):
    # checking the dataset order of operations selected
    dataset = config["data_type"]
    operations = config["operations"]
    config = config[dataset]
    # checking the dataset order of operations selected
    fileList = [file for file in os.popen('ls data/*.json').read().split('\n') if file]
    print(dataset, operations)

    # selecting appropriate pipeline
    if dataset == "medical": 
        try:
            if "dp" in operations:
                data, mean_absolute_error, noisy_query_output_for_epsilon_vector = medpipe.medicalPipelineDP(config, operations, fileList)
                data = utils.post_processing(data, config)
                formatted_error = utils.output_handler_medical_mae(mean_absolute_error, config)
                formatted_data = utils.output_handler_medical_dp_data(data, config)
                formatted_noise_vector = utils.output_handler_medical_noise_vector(noisy_query_output_for_epsilon_vector)
                concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error = formatted_error, noise_vector = formatted_noise_vector)
            if "k_anonymize" in operations:
                # if "suppress" in operations or "pseudonymize" in operations:
                data = medpipe.medicalPipelineSuppressPseudonymize(config, operations, fileList)
                k_anonymized_dataset, user_counts = medpipe.medicalPipelineKAnon(config, operations, fileList, data)  
                formatted_data = utils.output_handler_k_anon(k_anonymized_dataset, config)
                formatted_user_counts = utils.output_handler_k_anon(user_counts, config)
                concat_output = utils.output_concatenator(anonymised_output = formatted_data, user_counts = formatted_user_counts)
        except Exception as e:
            print("Error: ", e)
    if dataset == "spatioTemporal":
        try:
            if "dp" in operations:
                if config["differential_privacy"]["dp_query"] == 'mean':
                    data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
                    data = utils.post_processing(data, config)
                    mean_absolute_error = utils.mean_absolute_error(bVector)
                    formatted_error, formatted_averaged_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)          
                    formatted_data = utils.output_handlrer_spatioTemp_dp_data(data, config)
                    concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error_per_hat = formatted_error, epsilon_vs_averaged_error = formatted_averaged_error)
                if config["differential_privacy"]["dp_query"] == 'count':
                    data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
                    data = utils.post_processing(data, config)
                    mean_absolute_error = utils.mean_absolute_error(bVector)
                    formatted_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)
                    formatted_data = utils.output_handler_spatioTemp_dp_data(data, config)
                    concat_output = utils.output_concatenator(anonymised_output = formatted_data, epsilon_vs_error = formatted_error)
        except Exception as e:
            print("Error: ", e)
    return concat_output

