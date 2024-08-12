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
                data, mean_absolute_error = medpipe.medicalPipelineDP(config, operations, fileList)
                data = utils.post_processing(data, config)
                formatted_error = utils.output_handler_medical_mae(mean_absolute_error, config)
                formatted_data = utils.output_handler_medical_dp_data(data, config)

            if "k_anonymize" in operations:
                # if "suppress" in operations or "pseudonymize" in operations:
                data = medpipe.medicalPipelineSuppressPseudonymize(config, operations, fileList)
                k_anonymized_dataset, user_counts = medpipe.medicalPipelineKAnon(config, operations, fileList, data)  
                formatted_data = utils.output_handler_k_anon(k_anonymized_dataset, config)
                formatted_user_counts = utils.output_handler_k_anon(user_counts, config)
        except Exception as e:
            print("Error: ", e)
            formatted_data = []
    if dataset == "spatioTemporal":
        try:
            if "dp" in operations:
                data, bVector = stpipe.spatioTemporalPipeline(config, operations, fileList) 
                data = utils.post_processing(data, config)
                mean_absolute_error = utils.mean_absolute_error(bVector)
                formatted_error = utils.output_handler_spatioTemp_mae(mean_absolute_error, config)
                formatted_data = utils.output_handler_spatioTemp_dp_data(data, config)
                if "suppress" in operations:
                    formatted_data = stpipe.spatioTemporalPipeline(config, operations, fileList)
                if "pseudonymize" in operations:
                    formatted_data = stpipe.spatioTemporalPipeline(config, operations, fileList)
        except Exception as e:
            print("Error: ", e)
            formatted_data = []
    return formatted_data

    # TODO: Add output format handling (json dumps)
    # mods.output_handler(data, config)




# formatted_data = utils.output_handler(data)
