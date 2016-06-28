# -*- coding: utf-8 -*-
# !!!
# Get all of the attributes of Twitter followings and store in DSS dataset.

import dataiku
from dataiku.customrecipe import *
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
from common import get_client, calc_interval, get_userinfo

DEFAULT_INTERVAL=int(get_recipe_config()['default_interval'])
# For outputs, the process is the same:
output_dataset_name = get_output_names_for_role('main')[0]

client = get_client()

def get_friends():
    # get friends
    friends_ids = []

    cursor = -1
    while cursor != 0:
        try:
            # get friends
            print "Get friends list: "+str(cursor)
            response = client.api.friends.ids.get()

            # update cursor
            cursor = response.data.next_cursor

            # collect ids from response
            friends_ids = friends_ids + response.data.ids

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
    return friends_ids


following = get_friends()
print "Found %s following" % len(following)
nb_done = 0

results = []
for user in following:
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

    nb_done = nb_done + 1
    #if nb_done == 1:
    #    break

    print "Sleep "+str(interval)+" s"
    time.sleep(interval)

odf = pd.DataFrame(results)

if odf.size > 0:
    # Recipe outputs
    followers_info = dataiku.Dataset(output_dataset_name)
    followers_info.write_with_schema(odf)
