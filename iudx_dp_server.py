from flask import Flask, jsonify, request
from iudx_dp_main import main_process
import scripts.utilities as utils
import json
import  configparser

app = Flask(__name__)
server_config = configparser.ConfigParser()
server_config.read('server_config.cfg')


main_server_ip = server_config.get('DP_SERVER', 'ip')
main_server_port = server_config.get('DP_SERVER', 'port')
# db_server_ip = server_config.get('DB_SERVER', 'ip')
# db_server_port = server_config.get('DB_SERVER', 'port')


def send_response(response):
    output = {
        "output":response
    }
    ### start: to use only while testing the server ###
    with open('pipelineOutput/test_output.json','w') as f:
        json.dump(output, f) 
    print('response saved successfully')
    ### end: to use only while testing the server ###

    return output


@app.route("/test_server", methods = ["GET"])
def test_server():
    if (request.method == "GET"):
        data = "Test Server"
        return jsonify({'data': data})

@app.route("/process_dp", methods = ["POST"])
def process_dp():
    try:
        config = json.loads(request.get_data().decode())
        data = main_process(config)
        response = json.loads(data)
        response = send_response(response)
    except Exception as e:
        response = send_response(data)
    return response, 200


if __name__ == '__main__':
    # app.run(host=main_server_ip, port=main_server_port, debug=False)
    app.run(debug=True)
