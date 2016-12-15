# -*- coding: utf-8 -*-
import json
import dataiku
import requests
import datetime
from dataiku.customrecipe import *

#==============================================================================
# PLUGIN SETTINGS
#==============================================================================
input_names = get_input_names_for_role('input')
output_names = get_output_names_for_role('output')

INPUT_NAME  = input_names[0]
OUTPUT_NAME  = output_names[0]
ACCOUNT_ID_COL = get_recipe_config().get('account_id', None)
ACCESS_TOKEN_COL = get_recipe_config().get('access_token', None)


#==============================================================================
# PLUGIN MAIN CODE
#==============================================================================

# Recipe inputs
df = dataiku.Dataset(INPUT_NAME)

# Recipe outputs
out = dataiku.Dataset(OUTPUT_NAME)
schema = [{'name':'record', 'type':'string'}]
out.write_schema(schema)

# Persistent HTTP session
session = requests.Session()

# Base API 
API = 'https://api.instagram.com/v1'

# Main loop
w = out.get_writer()

for row in df.iter_rows():
    # Response container
    d = {}
    d['dku_params'] = {}
    d['dku_data'] = None
    d['dku_errors'] = None
    # Base params    
    d['dku_params']['fetched_at'] = datetime.datetime.utcnow().isoformat()
    d['dku_params']['account_id'] = row[ACCOUNT_ID_COL]
    d['dku_params']['access_token'] = row[ACCESS_TOKEN_COL]
    # Main API
    url = API + '/users/{}/'.format(row[ACCOUNT_ID_COL])
    params = {'access_token': row[ACCESS_TOKEN_COL]}
    r = session.get(url, params=params)
    try:
        d['dku_data'] = r.json()
    except Exception, e:
        d['dku_errors'] = str(e)
    w.write_row_dict( {'record': json.dumps(d)} )