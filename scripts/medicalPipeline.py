import medicalModules as medmod
import chunkHandlingModules as chmod
import utilities as utils

# for testing
medicalFileList = [
    "../data/syntheticMedicalChunks/medical_data_split_file_0.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_1.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_2.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_3.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_4.json"
]

operations = ["suppress", "pseudonymize"]
configDict = utils.read_config("../config/pipelineConfig.json")
medicalConfigDict = configDict["medical"]

# //TODO: Run all operation[category]s within a function
print("Performing common chunk accumulation functions")
dataframeAccumulate = chmod.chunkHandlingCommon(
    medicalConfigDict, operations, medicalFileList
)

print("Performing Chunk Accumulation for k-anon and DP")
dataframeAccumulateKAnon, dataframeAccumulateDP = chmod.chunkHandlingMedical(
    medicalConfigDict, operations, medicalFileList
)

# testing for differential privacy
print(dataframeAccumulateDP)

# testing k-anonymity
# print(dataframeAccumulateKAnon.to_string())
# optimalBinWidth = medmod.k_anonymize(dataframeAccumulateKAnon, medicalConfigDict)
# print("optimal bin width : ", optimalBinWidth)

privateAggregateDataframe = medmod.medicalDifferentialPrivacy(dataframeAccumulateDP, medicalConfigDict)
print(privateAggregateDataframe)