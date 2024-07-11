import scripts.spatioTemporalModules as stmod
import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils
import logging

# select logging level
logging.basicConfig(level = logging.INFO)


def spatioTemporalPipeline(config, operations, fileList):

    if "suppress" or "pseudonymize" in operations:
        logging.info("Performing common chunk accumulation functions")
        dataframeAccumulate = chmod.chunkHandlingCommon(config, 
                                                        operations, 
                                                    fileList)

    if "dp" in operations:
        logging.info("Performing Chunk Accumulation for DP")
        dataframeAccumulateDP, timeRange = chmod.chunkHandlingSpatioTemporal(config, 
                                                                fileList)

        logging.info("Performing Differential Privacy")
        privateAggregateDataframe, bVector = stmod.spatioTemporalDifferentialPrivacy(dataframeAccumulateDP,
                                                                        config, 
                                                                        timeRange)
    
    if "dp" in operations:
        return privateAggregateDataframe, bVector
    if ("suppress" or "pseudonymize") in operations and ("dp") not in operations:
        return dataframeAccumulate