import scripts.Modules as mod
import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils
import logging

# select logging level
logging.basicConfig(level = logging.INFO)


def generalPipelineDP(config, operations, fileList):
    
    logging.info("Performing Chunk Accumulation for DP")
    dataframeAccumulateDP = chmod.chunkHandlingGeneralDP(
        config, fileList
    )
    logging.info("Performing Differential Privacy")
    privateAggregateDataframe, bVector, noisy_query_output_for_epsilon_vector = mod.generalDifferentialPrivacy(dataframeAccumulateDP, config)
    return privateAggregateDataframe, bVector, noisy_query_output_for_epsilon_vector

        # utils.plot_normalised_mae(mean_normalised_mae, config)

