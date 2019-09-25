from flask import Flask
from flask import g
from flask import request
from flask_cors import CORS
from flask import Blueprint
from flask import jsonify
from dreamio_backend import DremIOClient
import time
# Configurations
DEBUG = True
BASE_URL = "http://localhost:9047"
username = "MSA"
password = "Password@123"

# App instantiation
app = Flask(__name__)
CORS(app)

msa = Blueprint('msa_poc', __name__, url_prefix='/msa_poc/v1')

@msa.route('/data', methods=['POST'])
def fetch_data():
    data = request.get_json()
    query = data.get("sql", None)
    client = DremIOClient(BASE_URL, username, password)
    print("TOKEN", client.token)
    job_id = "2275e9d6-bc8f-4820-7d07-7867ae2a0200" #client.create_job(query=query)
    print("Created Job ", job_id)
    #time.sleep(30)
    data = client.fetch_job_data(job_id)
    return jsonify(data), 200

def create_app():
    # Register Blueprints/Views.
    app.register_blueprint(msa)
    return app

def run_app():
    app = create_app()
    app.run(
        host="0.0.0.0",
        port=9000,
        debug=DEBUG
    )

if __name__ == '__main__':
    run_app()
