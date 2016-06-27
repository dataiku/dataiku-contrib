# Code for custom code recipe sonra_twitter_plugin_compute_following_info (imported from a Python recipe)

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

# For outputs, the process is the same:
output_dataset_name = get_output_names_for_role('main')[0]

#############################
# Your original recipe
#############################

# !!!
# Get all of the attributes of Twitter followers and store in DSS dataset.

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import time
import datetime 
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

# init API client
client = UserClient(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)


def calc_interval(headers):
    interval = DEFAULT_INTERVAL
    # if we hit the limit, wait for resetting time
    if 'x-rate-limit-remaining' in headers and headers['x-rate-limit-remaining'] <= 0:
        current_timestamp = int(time.time())
        interval += int(headers['x-rate-limit-reset']) - current_timestamp
    return interval

def get_followers():  
    # get followers
    followers_ids = []

    cursor = -1
    while cursor != 0:
        try:
            # get followers
            print "Get followers list: "+str(cursor)
            response = client.api.followers.ids.get()

            # update cursor
            cursor = response.data.next_cursor

            # collect ids from response
            followers_ids = followers_ids + response.data.ids

            # sleep between calls
            if ( cursor != 0 ) :
                interval = calc_interval(response.headers)
                print "Sleep "+str(interval)+" s"
                time.sleep(interval)

        except TwitterApiError, e:
            print "Exception: "+e._msg

            # set interval to 60 in case of error
            interval = 60
            print "Sleep "+str(interval)+" s"
            time.sleep(interval)
    return followers_ids

def get_userinfo(user):
    try:
        print "Get user's info: "+str(user)
        response = client.api.users.show.get( user_id=user )
        return response
    except TwitterApiError, e:
        print "Exception: "+e._msg
        
        # set interval to 60 in case of error
        interval = 60
        print "Sleep "+str(interval)+" s"
        time.sleep(interval)
    return False
        

followers = get_followers()
results = []
for user in followers:
    # get userdata
    response = get_userinfo(user)
    
    if response != False :
        # save to output dataset
        o = {}

        # save response status
        o["response_status"] = response.headers['Status']
        o["row_dtime"] = datetime.datetime.today().strftime("%m/%d/%Y %H:%M:%S")

        # save users data
        o["time_zone"] = response.data["time_zone"]
        o["id"] = response.data["id"]
        o["description"] = response.data["description"]
        o["followers_count"] = response.data["followers_count"]
        o["listed_count"] = response.data["listed_count"]
        o["lang"] = response.data["lang"]
        o["utc_offset"] = response.data["utc_offset"]
        o["statuses_count"] = response.data["statuses_count"]
        o["friends_count"] = response.data["friends_count"]
        o["name"] = response.data["name"]
        o["screen_name"] = response.data["screen_name"]
        o["url"] = response.data["url"]
        o["location"] = response.data["location"]
        o["created_at"] = str( datetime.datetime.strptime( response.data["created_at"].replace("+0000 ",""), "%a %b %d %H:%M:%S %Y" ) )

        o["id_str"] = response.data["id_str"]
        o["favourites_count"] = response.data["favourites_count"]
        o["geo_enabled"] = response.data["geo_enabled"]
        o["statuses_count"] = response.data["statuses_count"]
        o["following"] = response.data["following"]
        o["follow_request_sent"] = response.data["follow_request_sent"]
        o["notifications"] = response.data["notifications"]
        o["entities"] = json.dumps(response.data.entities)
        results.append(o)

        # calculate interval
        interval = calc_interval(response.headers)
    else:
        interval = DEFAULT_INTERVAL
    print "Sleep "+str(interval)+" s"
    time.sleep(interval)
    

odf = pd.DataFrame(results)

if odf.size > 0:
    # Recipe outputs
    followers_info = dataiku.Dataset(output_dataset_name)
    followers_info.write_with_schema(odf)
