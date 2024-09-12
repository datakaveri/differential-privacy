import json
import requests

params = {'k': 500, 'suppress_columns': 'Date of Birth,Address', 'pseudonymize_columns': 'Name,Patient ID', 'insensitive_columns': 'S.No,Patient ID,Name,Gender,Test Result,Blood Pressure', 'allow_record_suppression': 'True'}



# Step 3: Send the configuration to the service
url = 'http://192.168.1.232:8070/api/arx/process'
response = requests.get(url, params=params)

# Step 4: Handle the response
print("Response status:", response.json())
with open('anonymized_output_compare.json', 'w') as f:
    json.dump(json.loads(response.text), f)

