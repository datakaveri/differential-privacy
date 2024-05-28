import medicalModules as medmod
import chunkHandlingModules as chmod
import utilities as utils

# for testing
medicalFileList = [
    "../data/syntheticMedicalChunks/medical_data_split_file_0.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_1.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_2.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_3.json",
    "../data/syntheticMedicalChunks/medical_data_split_file_4.json",
]

operations = ["suppress", "pseudonymize"]
configDict = utils.read_config("../config/pipelineConfig.json")
medicalConfigDict = configDict["medical"]

print("Performing common chunk accumulation functions")
dataframeAccumulate = chmod.chunkHandlingCommon(
    medicalConfigDict, operations, medicalFileList
)

print("Performing Chunk Accumulation for k-anon")
dataframeAccumulateMed = chmod.chunkHandlingMedical(
    medicalConfigDict, operations, medicalFileList
)

# testing for spatio-temporal


# testing k-anonymity
# print(dataframeAccumulateMed.to_string())
# print(dataframeAccumulateMed["Count"].sum())
opt_bin_width = medmod.k_anonymize(dataframeAccumulateMed, medicalConfigDict)
print("optimal bin width : ", opt_bin_width)
