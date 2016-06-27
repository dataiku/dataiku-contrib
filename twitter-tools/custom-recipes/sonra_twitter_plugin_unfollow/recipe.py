# Code for custom code recipe sonra_twitter_plugin_compute_twitter_following_output (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_dataset_name = get_input_names_for_role('main')[0]

# For outputs, the process is the same:
output_dataset_name = get_output_names_for_role('main')[0]


# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
#my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
#my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

import datetime 
import time
import json
import os
import socket
import dataikuapi
# twitter client
from birdy.twitter import UserClient,TwitterApiError

def getAPIUrl():
    if "dataiku_url" not in get_recipe_config():
        dku_port = os.environ['DKU_BASE_PORT']
        host = socket.gethostname()
        return 'http://'+host+':'+dku_port
    else:
        return get_recipe_config()['dataiku_url']

def getConnection(name,key):
    APIUrl = getAPIUrl()
    print "API URL: "+APIUrl
    client = dataikuapi.dssclient.DSSClient(APIUrl,key)
    return client.list_connections()[name]['params']

twitter = getConnection(get_recipe_config()['connection_name'],get_recipe_config()['dataiku_token'])

# Twitter API keys
#CONSUMER_KEY=get_recipe_config()['consumer_key']
CONSUMER_KEY=twitter['api_key']
#CONSUMER_SECRET=get_recipe_config()['consumer_secret']
CONSUMER_SECRET=twitter['api_secret']
# User Access Keys
#ACCESS_TOKEN=get_recipe_config()['access_token']
ACCESS_TOKEN=twitter['token_key']
#ACCESS_TOKEN_SECRET=get_recipe_config()['access_token_secret']
ACCESS_TOKEN_SECRET=twitter['token_secret']


# Delay in seconds
DEFAULT_INTERVAL=int(get_recipe_config()['default_interval'])

# input dataset's column
INPUT_COLUMN=get_recipe_config()['input_column']

# init API client
client = UserClient(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

# user_data dataset
ud = dataiku.Dataset(input_dataset_name)

# output dataset
results = []

def calc_interval(headers):
    interval = DEFAULT_INTERVAL
    # if we hit the limit, wait for resetting time
    if 'x-rate-limit-remaining' in headers and headers['x-rate-limit-remaining'] <= 0:
        current_timestamp = int(time.time())
        interval += int(headers['x-rate-limit-reset']) - current_timestamp
    return interval

def unfollow(user):
    print "Unfollow: "+str(user)
    response = client.api.friendships.destroy.post( screen_name=user )
    return response

# for each user
for record in ud.iter_rows():
    o = {}
    # unfollow user
    userdata = unfollow( record[INPUT_COLUMN] )
    
    # save results in output dataset
    o["row_dtime"] = datetime.datetime.today().strftime("%m/%d/%Y %H:%M:%S")
    o["screen_name"] = userdata.data.screen_name
    o["name"] = userdata.data.name
    results.append(o)
    
    # interval between calls
    interval = calc_interval(userdata.headers)
    print "Sleep "+str(interval)+" s"
    time.sleep(interval)

odf = pd.DataFrame(results)

if odf.size > 0:
    # Recipe outputs
    twitter_output = dataiku.Dataset(output_dataset_name)
    twitter_output.write_with_schema(odf)
