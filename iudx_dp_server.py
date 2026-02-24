import json

from flask import Flask, jsonify, request

from iudx_dp_main import main_process

app = Flask(__name__)


@app.route("/test_server", methods=["GET"])
def test_server():
    return jsonify({"data": "Test Server"})


@app.route("/process_dp", methods=["POST"])
def process_dp():
    try:
        config = json.loads(request.get_data().decode())
        response = main_process(config)
    except Exception as e:
        response = {
            "status": "error",
            "error": {"code": "INTERNAL_ERROR", "message": str(e)},
        }
    return response, 200


if __name__ == "__main__":
    app.run(debug=True)
