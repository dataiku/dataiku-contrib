# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *
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
from common import get_client, calc_interval

input_dataset_name = get_input_names_for_role('main')[0]
output_dataset_name = get_output_names_for_role('main')[0]

client = get_client()

# input dataset's column
INPUT_COLUMN=get_recipe_config()['input_column']

# Recipe inputs
users_keywords = dataiku.Dataset(input_dataset_name)

# output dataset
results = []

# for each user
for record in users_keywords.iter_rows():
    kw = record.get(INPUT_COLUMN, None)
    if kw is None or len(kw) == 0:
        print "Empty keyword, ignoring"
        continue
    print "Searching: %s" % kw
    prev_response = [];
    # search for user
    try:
        # pagination
        cur_page=1
        do_break = False
        while True:
            print "Fetching page: "+str(cur_page)
            response = client.api.users.search.get(count=20,q=kw,page=cur_page)

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
            if cur_page == 2:
                print "Breaking on too many pages"
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
