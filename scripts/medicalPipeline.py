import scripts.medicalModules as medmod
import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils

def medicalPipeline(config, operations, fileList):
    if ("suppress" or "pseudonymize") in operations:
        print("Performing common chunk accumulation functions")
        dataframeAccumulate = chmod.chunkHandlingCommon(
            config, operations, fileList
        )

    if "k_anonymize" in operations:
        print("Performing Chunk Accumulation for k-anon")
        dataframeAccumulateKAnon = chmod.chunkHandlingMedicalKAnon(
            config, fileList
        )
        print("Performing k-anonymization")
        optimalBinWidth = medmod.k_anonymize(dataframeAccumulateKAnon, config)


    if "dp" in operations:
        print("Performing Chunk Accumulation forDP")
        dataframeAccumulateDP = chmod.chunkHandlingMedicalDP(
            config, fileList
        )
        print("Performing Differential Privacy")
        privateAggregateDataframe, bVector = medmod.medicalDifferentialPrivacy(dataframeAccumulateDP, config)

        mean_normalised_mae = utils.mean_absolute_error(dataframeAccumulateDP, bVector)  
        utils.plot_normalised_mae(mean_normalised_mae, config)

    if "dp" in operations:
        return privateAggregateDataframe
    if ("suppress" or "pseudonymize") in operations and ("dp" or "k_anonymize") not in operations:
        return dataframeAccumulate
    if "k_anonymize" in operations:
        return optimalBinWidth
