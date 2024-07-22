import scripts.medicalModules as medmod
import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils
import logging

# select logging level
logging.basicConfig(level = logging.INFO)

def medicalPipelineSuppressPseudonymize(config, operations, fileList):
    logging.info("Performing common chunk accumulation functions")
    dataframeAccumulate = chmod.chunkHandlingCommon(
        config, operations, fileList
    )
    return dataframeAccumulate

def medicalPipelineKAnon(config, operations, fileList):
    logging.info("Performing Chunk Accumulation for k-anon")
    dataframeAccumulateKAnon = chmod.chunkHandlingMedicalKAnon(
        config, fileList
    )
    logging.info("Performing k-anonymization")
    optimal_bin_width = medmod.k_anonymize(dataframeAccumulateKAnon, config)
    data = medmod.user_assignment_k_anonymize(optimal_bin_width, dataframeAccumulateKAnon, config)
    return data

def medicalPipelineDP(config, operations, fileList):
    logging.info("Performing Chunk Accumulation for DP")
    dataframeAccumulateDP = chmod.chunkHandlingMedicalDP(
        config, fileList
    )
    logging.info("Performing Differential Privacy")
    privateAggregateDataframe, bVector = medmod.medicalDifferentialPrivacy(dataframeAccumulateDP, config)
    return privateAggregateDataframe, bVector

        # utils.plot_normalised_mae(mean_normalised_mae, config)

