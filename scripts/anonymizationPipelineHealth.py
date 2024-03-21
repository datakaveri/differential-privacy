# script for function call handling 
import anonymizationPipelineHealthModules as mods

# necessary file reads
config = mods.read_config("pipelineConfig.json")
data = mods.read_data(config["data_file"])

# checking the order of operations required
operations = mods.oop_handler(config)
print(operations)

if "suppress" in operations:
    print("Performing Attribute Suppression")
    data = mods.suppress(data, config)
if "pseudonymize" in operations:
    print("Performing Attribute Pseudonymization: Pseudonymized Attribute stored in Hashed Value Column")
    data = mods.pseudonymize(data, config)
# if "k_anonymize" in operations:
#     print("K-Anonymizing Data")
#     data = mods.k_anonymize(data, config)
if "dp" in operations:
    print("Enacting Differential Privacy")
    data = mods.differential_privacy(data, config)

print(data)
print(data.head())
