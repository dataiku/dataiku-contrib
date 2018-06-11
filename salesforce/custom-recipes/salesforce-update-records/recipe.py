# Code for custom code recipe SF_writer (imported from a Python recipe)

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
input_dataset = get_input_names_for_role('main')[0]
# The dataset objects themselves can then be created like this:
input_df = dataiku.Dataset(input_dataset).get_dataframe()





#############################
# Your original recipe
#############################

# -*- coding: utf-8 -*-
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import json
import requests


COLUMNS=get_recipe_config()['COLUMNS']
ID_COLUMN=get_recipe_config()['ID_COLUMN']
SF_OBJECT=get_recipe_config()['SF_OBJECT']
FILE_TOKEN=get_recipe_config()['token']

print get_recipe_config()
print COLUMNS

#FILE_TOKEN=dataiku.get_custom_variables()['dip.home']+'/salesforce/sales_cloud_token.json'


token = salesforce.get_json(FILE_TOKEN)
ACCESS_TOKEN = token.get("access_token")
API_BASE_URL = token.get("instance_url")
s = requests.Session()

def sf_api_call(action, parameters = {}, method = 'get', data = {}):
    """
    Makes an API call to SalesForce
    Parameters: action (the URL), params, method (GET or POST), data for POST
    """
    headers = {
        'Content-type': 'application/json',
        'Accept-Encoding': 'gzip',
        'Authorization': 'Bearer %s' % ACCESS_TOKEN
    }
    if method == 'get':
        r = s.request(method, API_BASE_URL+action, headers=headers, params=parameters, timeout=30)
    elif method == 'post':
        r = s.request(method, API_BASE_URL+action, headers=headers, data=data, params=parameters, timeout=10)
    elif method == 'patch':
        r = s.request(method, API_BASE_URL+action, headers=headers, data=data, params=parameters, timeout=10)
    else:
        raise ValueError('Method should be get or post.')
    print('API %s call: %s' % (method, r.url) )
    if ((r.status_code == 200 and method == 'get') or (r.status_code == 201 and method == 'post') or (r.status_code == 204 and method == 'patch')):
        if method=='patch':
            return 'success'
        else:
            return r.json()
    else:
        return 'error: %s' % (r.content)
        

input_df['updated']=''
input_df['error']=''

for index,row in input_df.iterrows():
    properties={}
    for col in COLUMNS:
        if not pd.isnull(row[col]):
            properties[col]=row[col]
    
    print 'calling SF to update %s with %s' % ( row[ID_COLUMN],properties)
    a= sf_api_call('/services/data/v39.0/sobjects/%s/%s'%(SF_OBJECT,row[ID_COLUMN]), method="patch", data=json.dumps(properties))
    if a== 'success':
        input_df.loc[index,'updated']=json.dumps(properties)
    else:
        input_df.loc[index,'error']=a
    print a


# Recipe outputs
main_output_name = get_output_names_for_role('main')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(input_df)
