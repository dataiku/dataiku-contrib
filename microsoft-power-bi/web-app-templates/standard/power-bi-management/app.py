import base64
import dataiku
import requests
import datetime
import pandas as pd
from flask import request
from Crypto.Cipher import AES
from Crypto.Hash import SHA256


global_IV = 'to be randomized'


@app.route("/display-new-token")
def display_new_token():    
    data = {
        "username"     : request.args.get("powerbi-username"),
        "password"     : request.args.get("powerbi-password"),
        "client_id"    : request.args.get("powerbi-client-id"),
        "client_secret": request.args.get("powerbi-client-secret"),
        "resource"     : request.args.get("powerbi-resource",   "https://analysis.windows.net/powerbi/api"),
        "grant_type"   : request.args.get("powerbi-grant-type", "password"),
        "scope"        : request.args.get("powerbi-scope",      "openid")
    }
    response = requests.post('https://login.microsoftonline.com/common/oauth2/token', data=data)
    o = {}
    #o["powerbi-auth-response"] = response.json()
    o["powerbi-access-token"] = response.json().get("access_token")    
    return json.dumps(o)


# Helper functions to interact with DSS Project Variables
def set_dss_variables(project_key, pbi_data, variables_type="standard"):
    dss = dataiku.api_client()
    project = dss.get_project(project_key)
    variables = project.get_variables()
    variables[variables_type]['powerbi-settings'] = pbi_data
    project.set_variables(variables)
    
    
# Helper functions for encryption    
def decrypt_string(cipher, key):
    cipher = base64.b64decode(cipher)
    key = SHA256.new(key).digest()
    decryptor = AES.new(key, AES.MODE_CBC, global_IV)
    message = decryptor.decrypt(cipher)
    #TODO: implement more robust padding
    return message.rstrip()


def encrypt_string(message, key):
    #message = 'this is my message'
    #TODO: implement more robust padding
    padding = (16 - (len(message) % 16))
    padded_message = message + ' ' * padding
    key = SHA256.new(key).digest()
    #IV = Random.new().read(AES.block_size)
    encryptor = AES.new(key, AES.MODE_CBC, global_IV)
    cipher = encryptor.encrypt(padded_message)
    return base64.b64encode(cipher)


@app.route("/save-new-token", methods=['POST'])
def save_new_token():
    
    # Read in the conf and get a token
    conf = json.loads(request.data)
    key = conf["api-key"]
    pbi = {}
    pbi["username"]      = conf["powerbi-username"]
    pbi["password"]      = conf["powerbi-password"]
    pbi["client_id"]     = conf["powerbi-client-id"]
    pbi["client_secret"] = conf["powerbi-client-secret"]
    pbi["resource"]      = conf["powerbi-resource"]
    pbi["grant_type"]    = conf["powerbi-grant-type"]
    pbi["scope"]         = conf["powerbi-scope"]
    response = requests.post('https://login.microsoftonline.com/common/oauth2/token', data=pbi)
        
    # Save the token
    data = pbi
    data["password"]       = encrypt_string(conf["powerbi-password"], key)
    data["client_secret"]  = encrypt_string(conf["powerbi-client-secret"], key)
    data["access_token"]   = response.json().get("access_token")
    data["created_at"]     = str(datetime.datetime.utcnow())
    data["dss_port"]       = os.environ["DKU_BACKEND_PORT"]
    data["webapp_project"] = conf["webapp-url"].split("/")[-3]
    data["webapp_id"]      = conf["webapp-url"].split("/")[-2]
    data["project_key"]    = os.environ["DKU_CURRENT_PROJECT_KEY"]
    
    set_dss_variables(
        dataiku.default_project_key(),
        data
    )
    # Send back some results
    o = {}
    o["powerbi-access-token"] = data["access_token"]
    return json.dumps(o)


@app.route("/get-existing-credentials")
def get_existing_credentials():
    
    # Read in the existing conf
    dss = dataiku.api_client()
    project = dss.get_project(dataiku.default_project_key())
    variables = project.get_variables()["standard"]
    conf = variables.get("powerbi-settings", None)
    
    # Decrypt
    key = request.args.get("api-key")
    pbi = {}
    pbi["powerbi-username"]      = conf["username"]
    pbi["powerbi-password"]      = decrypt_string(conf["password"], key)
    pbi["powerbi-client-id"]     = conf["client_id"]
    pbi["powerbi-client-secret"] = decrypt_string(conf["client_secret"], key)
    pbi["powerbi-resource"]      = conf["resource"]
    pbi["powerbi-grant-type"]    = conf["grant_type"]
    pbi["powerbi-scope"]         = conf["scope"]
    
    # Send back some results
    return json.dumps(pbi)


@app.route("/get-token")
def get_token():
    
    # Read in the existing conf
    dss = dataiku.api_client()
    project = dss.get_project(dataiku.default_project_key())
    variables = project.get_variables()["standard"]
    conf = variables.get("powerbi-settings", None)   
    
    # Decrypt
    key = request.args.get("api-key")
    pbi = {}
    pbi["username"]      = conf["username"]
    pbi["password"]      = decrypt_string(conf["password"], key)
    pbi["client_id"]     = conf["client_id"]
    pbi["client_secret"] = decrypt_string(conf["client_secret"], key)
    pbi["resource"]      = conf["resource"]
    pbi["grant_type"]    = conf["grant_type"]
    pbi["scope"]         = conf["scope"]
    
    # Get the token
    response = requests.post('https://login.microsoftonline.com/common/oauth2/token', data=pbi)
    o = {}
    o["token"] = response.json().get("access_token") 
    
    return json.dumps(o)