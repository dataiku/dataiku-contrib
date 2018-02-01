import requests
from flask import request, Response
import dataiku
dataiku.use_plugin_libs("deeplearning-image")
from tensorboard_handle import TensorboardThread
import time
import os

###################################################################################################################
## VARIABLES THAT NEED TO BE SET
###################################################################################################################

# To work, your web-app requires to run on a code-env with the following libraries installed:
# tensorflow==1.4.0
# flask==0.12.2

# The 'model_folder' must be the name of the managed folder where tensorboard logs are found.
# They are generated through the Retrain recipe, when checking the 'tensorboard' option
model_folder = "retrained_model"

###################################################################################################################
## DEFINING AND LAUNCHING TENSORBOARD
###################################################################################################################

tt = TensorboardThread(model_folder)
port = tt.get_port()
tt.start()

###################################################################################################################
## ROUTING
###################################################################################################################

@app.route('/tensorboard-endpoint')
def tensorboard_endpoint():
    url = "http://localhost:{}/".format(port)
    return resp_from_url(url)

@app.route('/data/<path:url>')
def proxy(url):
    redirect_url = "http://localhost:{}/data/{}".format(port, url)
    response = requests.get(redirect_url, stream=True, params=request.args)
    return resp_from_url(redirect_url)

def resp_from_url(url):
    response = requests.get(url, stream=True, params=request.args)
    return response.content
