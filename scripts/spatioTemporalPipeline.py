import scripts.spatioTemporalModules as stmod
import scripts.chunkHandlingModules as chmod

def spatioTemporalPipeline(config, operations, fileList):

    if "suppress" or "pseudonymize" in operations:
        print("Performing common chunk accumulation functions")
        dataframeAccumulate = chmod.chunkHandlingCommon(config, 
                                                        operations, 
                                                    fileList)

    if "dp" in operations:
        print("Performing Chunk Accumulation for DP")
        dataframeAccumulateDP = chmod.chunkHandlingSpatioTemporal(config, 
                                                                fileList)
        timeRange = stmod.timeRange(dataframeAccumulateDP)

        print("Performing Differential Privacy")
        privateAggregateDataframe = stmod.spatioTemporalDifferentialPrivacy(dataframeAccumulateDP, 
                                                                        config, 
                                                                        timeRange)
    if "dp" in operations:
        print_output = print("Returning privateAggregateDataframe")
        return privateAggregateDataframe, print_output
    if ("suppress" or "pseudonymize") in operations and ("dp") not in operations:
        print_output = print("Returning dataframeAccumulate")
        return dataframeAccumulate, print_output