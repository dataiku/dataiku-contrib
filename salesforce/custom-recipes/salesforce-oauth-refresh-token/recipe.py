# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *
import json
#import salesforce
import requests
import datetime

# Output
output_name = get_output_names_for_role('main')[0]
output = dataiku.Dataset(output_name)
output.write_schema([
    {'name':'datetime', 'type':'string'},
    {'name':'status_code', 'type':'string'},
    {'name':'result', 'type':'string'},
    {'name':'path_file', 'type':'string'}
])

# Read configuration
config = get_recipe_config()
client_id = config.get('client_id', '')
client_secret = config.get('client_secret', '')
email = config.get('email', '')
password = config.get('password', '')
sandbox = config.get('sandbox', False)
path = config.get('path', None)
hide_access_token = config.get('hide_access_token', True)

# Request
now = datetime.datetime.now()
params = {
    "grant_type": "password",
    "client_id": client_id,
    "client_secret": client_secret,
    "username": email,
    "password": password
}

if sandbox:
    url = "https://test.salesforce.com/services/oauth2/token"
else:
    url = "https://login.salesforce.com/services/oauth2/token"

r = requests.post(url, params=params)

# Hide if required
if hide_access_token:
    try:
        result_in_dataset = r.json()
        if "access_token" in result_in_dataset.keys():
            result_in_dataset['access_token'] = '***HIDDEN***'
        result_in_dataset = json.dumps(result_in_dataset)
    except Exception as e:
        result_in_dataset = r.text
else:
    result_in_dataset = r.text

# Write result in dataset
writer = output.get_writer()
writer.write_row_dict({
    'datetime': str(now),
    'status_code': r.status_code,
    'result': result_in_dataset,
    'path_file': path
    })
writer.close()

# Writing in file only if API call is successful
if r.status_code == 200:
    file = open(path, 'w')
    file.write(r.text)
    file.close()
