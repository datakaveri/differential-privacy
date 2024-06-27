# import statements
import scripts.medicalPipeline as medpipe
import scripts.spatioTemporalPipeline as stpipe
import scripts.utilities as utils

# ? Importing datasets
medicalFileList = ["data/syntheticMedicalChunks/medical_data_split_file_0.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_1.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_2.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_3.json",
                    "data/syntheticMedicalChunks/medical_data_split_file_4.json"]
    
spatioTemporalFileList = ['data/spatioTemporalChunks/split_file_0.json',
                    'data/spatioTemporalChunks/split_file_1.json',
                    'data/spatioTemporalChunks/split_file_2.json',
                    'data/spatioTemporalChunks/split_file_3.json',
                    'data/spatioTemporalChunks/split_file_4.json']

# ? Config handler
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

# ? Run pipeline method that is being called by the Flask app
def run_pipeline(request_body):

    operations = utils.oop_handler(request_body)
    dataset, config, fileList = dataset_handler(request_body)

    if dataset == "medical":
        data = medpipe.medicalPipeline(config, operations, fileList)
    if dataset == "spatioTemporal":
        data = stpipe.spatioTemporalPipeline(config, operations, fileList)


    # data = data.head(10000)
    response = data.to_dict(orient='records')
    return response
