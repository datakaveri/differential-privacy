import json
import requests

# Step 1: Read the configuration file
config_file_path = "./config.json"

with open(config_file_path, 'r') as config_file:
    config = json.load(config_file)

# Step 2: Modify the config if needed
config['medical']['k_anonymize']['k'] = 500
config['medical']['allow_record_suppression'] = "True"

# Prepare the parameters to send to the service
params = {
    "k": config['medical']['k_anonymize']['k'],
    "suppress_columns": ','.join(config['medical']['suppress']),
    "pseudonymize_columns": ','.join(config['medical']['pseudonymize']),
    "insensitive_columns": ','.join(config['medical']['insensitive_columns']),
    "allow_record_suppression": config['medical']['allow_record_suppression']
}
print(params)
# Step 3: Send the configuration to the service
url = 'http://192.168.1.232:8070/api/arx/process'
response = requests.get(url, params=params)

# Step 4: Handle the response
print("Response status:", response.json())
with open('anonymized_output_compare.json', 'w') as f:
    json.dump(json.loads(response.text), f)
