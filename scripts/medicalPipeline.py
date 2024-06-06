import scripts.medicalModules as medmod
import scripts.chunkHandlingModules as chmod


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
        privateAggregateDataframe = medmod.medicalDifferentialPrivacy(dataframeAccumulateDP, config)

    if "dp" in operations:
        return privateAggregateDataframe
    if ("suppress" or "pseudonymize") in operations and ("dp" or "k_anonymize") not in operations:
        return dataframeAccumulate
    if "k_anonymize" in operations:
        return optimalBinWidth
