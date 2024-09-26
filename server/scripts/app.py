import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import gzip
import configparser
import requests


app = Flask(__name__)
CORS(app, 
    origins="*",
    allow_headers="*"
)

server_config = configparser.ConfigParser()
server_config.read('scripts/server_config.cfg')


@app.route("/get_dataset_names", methods=['GET'])
def get_dataset_names():
    url = server_config.get('RESOURCE_SERVER', 'url')+"uploads"
    username=server_config.get('RESOURCE_SERVER', 'username')
    password=server_config.get('RESOURCE_SERVER', 'password')
    headers = {
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers, auth=(username, password))
    data = response.json()
    filenames = data.get('files', [])

    if response.status_code == 200:
        print('Fetched dataset names successfully.')
    else:
        print('Failed to fetch dataset names. Status code:', response.status_code)
    return jsonify(filenames), 200

@app.route('/run_dp_pipeline', methods=['POST'])
def dp_run():

    if request.is_json:
        json_data = request.get_json()
        dp_server_url = server_config.get('DP_SERVER', 'url')+"process_dp"
        # print(json_data)
        response = requests.post(dp_server_url, data=json.dumps(json_data))
        res = json.loads(response.text)
        return res, 200

    else:
        return jsonify({"error": "Request must be in JSON format"}), 400

@app.route('/run_k_anon_pipeline', methods=['POST'])
def k_anon_run():
    
    if request.is_json:
        json_data = request.get_json()
        dataset = json_data["data_type"]
        config = json_data[dataset]
        
        # ? k Anon parameters modified
        k_anon_params = {
                    "k": config['k_anonymize']['k'],
                    "suppress_columns": ','.join(config['suppress']),
                    "pseudonymize_columns": ','.join(config['pseudonymize']),
                    "generalized_columns": ','.join(config['generalize']),
                    "insensitive_columns": ','.join(config['insensitive_columns']),
                    "widths":config['width'],
                    "num_levels":config['levels'],
                    "allow_record_suppression": config['allow_record_suppression']
                }
        
        k_anon_server_url = server_config.get('K_ANON_SERVER', 'url')+"api/arx/process"
        # print(json_data)
        headers = {
                    'Content-Type': 'application/json'
                }
        response = requests.post(k_anon_server_url, headers= headers, data=json.dumps(k_anon_params))
        res = json.loads(response.text)
        return res, 200

@app.route('/save_config', methods=['POST'])
def save_config():

    if request.is_json:
        json_data = request.get_json()
        dataset_name = json_data['dataset_name']
        ####### Start: save config to rs #######
        rs_url = server_config.get('RESOURCE_SERVER', 'url')+"config/:"+dataset_name
        dataset_name = json_data['dataset_name']
        username=server_config.get('RESOURCE_SERVER', 'username')
        password=server_config.get('RESOURCE_SERVER', 'password')
        headers = {
            "Content-Type": "application/json"
        }

        config_response = requests.post(rs_url, headers=headers, auth=(username, password), data=json.dumps(json_data))

        if config_response.status_code == 204:
            print('config File uploaded successfully.')
            
        else:
            print('Failed to upload file. Status code:', config_response.status_code)          
        
        ###### End: save config to rs #######
        return 'success'
    else:
        return jsonify({"error": "Request must be in JSON format"}), 400

app.run(debug=True)