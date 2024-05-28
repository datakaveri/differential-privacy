# import statements
import scripts.medicalPipeline as medmod
import scripts.spatioTemporalModules as stmod
import scripts.chunkHandlingModules as chmod
import scripts.utilities as utils

# //TODO: Add logic for pipeline selection from config
# necessary file reads
config = utils.read_config("../config/pipelineConfig.json")
data = utils.read_data(config["data_file"])

# checking the order of operations required
operations = utils.oop_handler(config)
print(operations)

if "suppress" in operations:
    print("Performing Attribute Suppression")
    data = chmod.suppress(data, config)
if "pseudonymize" in operations:
    print("Performing Attribute Pseudonymization: Pseudonymized Attribute stored in Hashed Value Column")
    data = chmod.pseudonymize(data, config)
if "k_anonymize" in operations:
    print("K-Anonymizing Data")
    data, users_per_bin = mods.k_anonymize(data, config)
if "dp" in operations:
    print("Enacting Differential Privacy")
    data = mods.differential_privacy(data, config)

# print(data)
mods.output_handler(data, config)