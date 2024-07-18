import scripts.medicalModules as medmod
import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils

def medicalPipelineSuppressPseudonymize(config, operations, fileList):
    print("Performing common chunk accumulation functions")
    dataframeAccumulate = chmod.chunkHandlingCommon(
        config, operations, fileList
    )
    return dataframeAccumulate

def medicalPipelineKAnon(config, operations, fileList):
    print("Performing Chunk Accumulation for k-anon")
    dataframeAccumulateKAnon = chmod.chunkHandlingMedicalKAnon(
        config, fileList
    )
    print("Performing k-anonymization")
    optimalBinWidth = medmod.k_anonymize(dataframeAccumulateKAnon, config)
    return optimalBinWidth

def medicalPipelineDP(config, operations, fileList):
    print("Performing Chunk Accumulation for DP")
    dataframeAccumulateDP = chmod.chunkHandlingMedicalDP(
        config, fileList
    )
    print("Performing Differential Privacy")
    privateAggregateDataframe, bVector = medmod.medicalDifferentialPrivacy(dataframeAccumulateDP, config)
    return privateAggregateDataframe, bVector

        # utils.plot_normalised_mae(mean_normalised_mae, config)

