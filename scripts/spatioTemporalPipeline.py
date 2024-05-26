import spatioTemporalModules as stmod
import chunkHandlingModules as chmod
import utilities as utils
# for testing
spatioTemporalFileList = ['../data/spatioTemporalChunks/split_file_0.json',
            '../data/spatioTemporalChunks/split_file_1.json',
            '../data/spatioTemporalChunks/split_file_2.json',
            '../data/spatioTemporalChunks/split_file_3.json',
            '../data/spatioTemporalChunks/split_file_4.json',
            '../data/spatioTemporalChunks/split_file_5.json',
            '../data/spatioTemporalChunks/split_file_6.json',
            '../data/spatioTemporalChunks/split_file_7.json',
            '../data/spatioTemporalChunks/split_file_8.json',
            '../data/spatioTemporalChunks/split_file_9.json']

operations = ["suppress", "pseudonymize"]
configDict = utils.read_config("../config/pipelineConfig.json")
spatioTemporalConfigDict = configDict["spatioTemporal"]


dataframeAccumulate = chmod.chunkHandlingCommon(spatioTemporalConfigDict, 
                                                operations, 
                                                spatioTemporalFileList)

dataframeAccumulateST = chmod.chunkHandlingSpatioTemporal(spatioTemporalConfigDict, 
                                                          operations, 
                                                          spatioTemporalFileList)

timeRange = stmod.timeRange(dataframeAccumulateST)

privateAggregateDataframe = stmod.spatioTemporalDifferentialPrivacy(dataframeAccumulateST, 
                                                                    spatioTemporalConfigDict, 
                                                                    timeRange)
