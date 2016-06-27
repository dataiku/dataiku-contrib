# Code for custom code recipe sonra_twitter_plugin_compute_users_from_keywords (imported from a Python recipe)

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
#input_A_names = get_input_names_for_role('input_A_role')
# The dataset objects themselves can then be created like this:
#input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# For outputs, the process is the same:
#output_A_names = get_output_names_for_role('main_output')
#output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


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

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_dataset_name = get_input_names_for_role('main')[0]

# For outputs, the process is the same:
output_dataset_name = get_output_names_for_role('main')[0]


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
from birdy.twitter import UserClient,TwitterApiError,ApiResponse

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

# Interval in seconds
DEFAULT_INTERVAL=int(get_recipe_config()['default_interval'])

# input dataset's column
INPUT_COLUMN=get_recipe_config()['input_column']

def calc_interval(headers):
    interval = DEFAULT_INTERVAL
    # if we hit the limit, wait for resetting time
    if headers['x-rate-limit-remaining'] <= 0:
        current_timestamp = int(time.time())
        interval += int(headers['x-rate-limit-reset']) - current_timestamp
    return interval

# init API client
client = UserClient(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)


# Recipe inputs
users_keywords = dataiku.Dataset(input_dataset_name)

# output dataset
results = []

# for each user
for record in users_keywords.iter_rows():
    print "Searching: "+record[INPUT_COLUMN]
    interval = DEFAULT_INTERVAL 
    prev_response = [];
    # search for user
    try:
        # pagination
        cur_page=1
        do_break = False
        while True:
            print "Current page: "+str(cur_page)
            response = client.api.users.search.get(count=20,q=record[INPUT_COLUMN],page=cur_page)

            if len(response.data) == 0:
                print "Results: 0"
                break
            elif len(response.data) < 20:
                print "Less than 20 results on page, assuming last page"
                do_break=True
            elif isinstance(prev_response, ApiResponse) and prev_response.data[0]["id"]==response.data[0]["id"]:
                print "Current page is similar to previous, skip results"
                break

            print "Status: "+response.headers['Status']
        
            for user in response.data:
                o = {}
                # save keywords
                o["search_keywords"] = record[INPUT_COLUMN]
                
                # save response status
                o["response_status"] = response.headers['Status']
                #o["response_headers"] = response.headers
                #o["response_data"] = json.dumps(response.data)
                
                o["row_dtime"] = datetime.datetime.today().strftime("%m/%d/%Y %H:%M:%S")
                
                # save users data
                o["time_zone"] = user["time_zone"]
                o["id"] = user["id"]
                o["description"] = user["description"]
                o["followers_count"] = user["followers_count"]
                o["listed_count"] = user["listed_count"]
                o["lang"] = user["lang"]
                o["utc_offset"] = user["utc_offset"]
                o["statuses_count"] = user["statuses_count"]
                o["friends_count"] = user["friends_count"]
                o["name"] = user["name"]
                o["screen_name"] = user["screen_name"]
                o["url"] = user["url"]
                o["location"] = user["location"]
                #o["created_at"] = user["created_at"]
                o["created_at"] = str( datetime.datetime.strptime( user["created_at"].replace("+0000 ",""), "%a %b %d %H:%M:%S %Y" ) )
                
                o["id_str"] = user["id_str"]
                o["favourites_count"] = user["favourites_count"]
                o["geo_enabled"] = user["geo_enabled"]
                o["statuses_count"] = user["statuses_count"]
                o["following"] = user["following"]
                o["follow_request_sent"] = user["follow_request_sent"]
                o["notifications"] = user["notifications"]
                o["entities"] = json.dumps(user.entities)
                
                results.append(o)
                
            # calculate interval
            interval = calc_interval(response.headers)

            cur_page = cur_page + 1

            # exit
            if do_break:
                break
            prev_response = response
            
            print "Sleep "+str(interval)+" s"
            time.sleep(interval)
            
    except TwitterApiError, e:
        print "Exception: "+e._msg
        
        # set interval to 60 in case of error
        interval = 60
        
        if e._msg == "Rate limit exceeded" :
            interval = 900
        
        print "Sleep "+str(interval)+" s"
        time.sleep(interval)
        
    prev_response = [];

odf = pd.DataFrame(results)

if odf.size > 0:
    # Recipe outputs
    users_from_keywords = dataiku.Dataset(output_dataset_name)
    users_from_keywords.write_with_schema(odf)
