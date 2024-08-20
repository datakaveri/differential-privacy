import json, requests
import scripts.utilities as utils
import pandas as pd


# necessary file reads
config_file_name = "config/pipelineConfig.json"

config = utils.read_config(config_file_name) 
url = 'http://127.0.0.1:55555/process_dp'

response = requests.post(url, data=json.dumps(config))

print("response status:", response.status_code)
# with open('pipelineOutput/test_output_compare.json','w') as f:
#     json.dump(json.loads(response.text), f) 

