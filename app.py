import json
from flask import Flask, request, jsonify
from flask_cors import CORS
import gzip

from run_pipeline import run_pipeline
from flask_compress import Compress

app = Flask(__name__)
CORS(app, 
    origins="*",
    allow_headers="*"
)

# ? Response compression
Compress(app)
app.config['COMPRESS_MIMETYPES'] = ['text/html', 'text/css', 'text/xml', 'application/json', 'application/javascript']
app.config['COMPRESS_LEVEL'] = 6  # Compression level (1-9)
app.config['COMPRESS_MIN_SIZE'] = 500  # 

@app.route('/run_pipeline', methods=['POST'])
def run():

    if request.is_json:
        request_body = request.get_json()
        return run_pipeline(request_body), 200

    else:
        return jsonify({"error": "Request must be in JSON format"}), 400


app.run(debug=True)

