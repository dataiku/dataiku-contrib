from dataiku.customwebapp import *

# Access the parameters that end-users filled in using webapp config
# For example, for a parameter called "input_dataset"
# input_dataset = get_webapp_config()["input_dataset"]

import dataiku
import pandas as pd
import glob
import base64

import simplejson as json

@app.route('/first_api_call')
def first_call():
    # TODO: Find a way to select the folder name
    input_folder = get_webapp_config()["replay_folder"]
    myfolder = dataiku.core.managed_folder.Folder(input_folder)
    
    myfolder_path = myfolder.get_path()
    
    
    json_files = glob.glob(myfolder_path + '/*.json')
    json_path = json_files[0]
    
    video_files = glob.glob(myfolder_path + '/*.mp4')
    video_path = video_files[0]
    
    with open(json_path) as f:
        data = json.load(f)
        print("data", data)
   
    with open(video_path, "rb") as video_file:
        video_encoded = encoded = base64.b64encode(video_file.read())
    
    jsonFile = {"data": data,
                      "video": video_encoded}

    return json.dumps(jsonFile)



   