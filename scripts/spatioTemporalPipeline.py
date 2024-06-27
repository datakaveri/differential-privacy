import scripts.spatioTemporalModules as stmod
import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils

def spatioTemporalPipeline(config, operations, fileList):

    if "suppress" or "pseudonymize" in operations:
        print("Performing common chunk accumulation functions")
        dataframeAccumulate = chmod.chunkHandlingCommon(config, 
                                                        operations, 
                                                    fileList)

    if "dp" in operations:
        print("Performing Chunk Accumulation for DP")
        dataframeAccumulateDP, timeRange = chmod.chunkHandlingSpatioTemporal(config, 
                                                                fileList)

        print("Performing Differential Privacy")
        privateAggregateDataframe, bVector = stmod.spatioTemporalDifferentialPrivacy(dataframeAccumulateDP, 
                                                                        config, 
                                                                        timeRange)
    
        mean_normalised_mae = utils.mean_absolute_error(dataframeAccumulateDP, bVector)
    
    if "dp" in operations:
        return privateAggregateDataframe
    if ("suppress" or "pseudonymize") in operations and ("dp") not in operations:
        return dataframeAccumulate