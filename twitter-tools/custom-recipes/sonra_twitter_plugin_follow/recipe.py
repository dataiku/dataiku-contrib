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
from birdy.twitter import UserClient,TwitterApiError
from common import get_client, calc_interval, get_userinfo

input_dataset_name = get_input_names_for_role('main')[0]
output_dataset_name = get_output_names_for_role('main')[0]

client = get_client()

# input dataset's column
INPUT_COLUMN=get_recipe_config()['input_column']

# user_data dataset
ud = dataiku.Dataset(input_dataset_name)

# output dataset
results = []

# for each user
for record in ud.iter_rows():
    user_id = record.get(INPUT_COLUMN, None)
    if user_id is None or len(user_id) == 0:
        print "Empty user id, ignoring"
        continue
    print "Follow user: "+user_id
    o = {}
    o["screen_name"] = user_id
    # follow user
    try:
        response = client.api.friendships.create.post(screen_name=user_id)
        print "Status: "+response.headers['Status']

        # save response status
        o["row_dtime"] = datetime.datetime.today().strftime("%m/%d/%Y %H:%M:%S")
        o["status"] = response.headers['Status']
        o["response_headers"] = response.headers
        o["response_data"] = json.dumps(response.data)

        interval = calc_interval(response.headers)
        print "Sleep "+str(interval)+" s"
        time.sleep(interval)

    except TwitterApiError, e:
        o["status"] = e._msg
        print "Exception: %s" % str(e)
    finally:
        results.append(o)


odf = pd.DataFrame(results)

if odf.size > 0:
    # Recipe outputs
    twitter_following_output = dataiku.Dataset(output_dataset_name)
    twitter_following_output.write_with_schema(odf)
