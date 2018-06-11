# Code for custom code recipe Create Object Records (imported from a Python recipe)

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
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import json
import requests
import math
import salesforce

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.


input_dataset = get_input_names_for_role('main')[0]
# The dataset objects themselves can then be created like this:
df = dataiku.Dataset(input_dataset).get_dataframe()



# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################


#FILE_TOKEN=dataiku.get_custom_variables()['dip.home']+'/salesforce/sales_cloud_token.json'
FILE_TOKEN=get_recipe_config()['token']
COLUMNS=get_recipe_config()['COLUMNS']
OBJECT=get_recipe_config()['SF_OBJECT']

token = salesforce.get_json(FILE_TOKEN)
ACCESS_TOKEN = token.get("access_token")
API_BASE_URL = token.get("instance_url")
s = requests.Session()


# Recipe inputs


for c in df:
    if df[c].dtype=='datetime64[ns]':
        df[c]=df[c].apply(lambda x: x.strftime('%Y-%m-%d'))
        
df['error']=''
df['data']=''
df['Id']=''

df=df[COLUMNS]

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
        if method=='patch' or method=='post':
            return ['success',r.json()]
        else:
            return r.json()
    else:
        raise ValueError('error: %s' % (r.content))

batch_list=[]
for index, row in df.iterrows():
    data={}
    for col in df.columns:
        if not pd.isnull(row[col]):
            data[col]=row[col]
    
    batch_list.append(data)
    call=sf_api_call('/services/data/v39.0/sobjects/%s/'%(OBJECT), method="post", data=json.dumps(data))
    
    
    if call[0]== 'success':
        df.loc[index,'data']=json.dumps(data)
        df.loc[index,'Id']=call[1].get('id')
    else:
        df.loc[index,'error']=call
    
"""
def chunkify(lst,n):
    return [lst[i::n] for i in xrange(n)]



for chunk in chunkify(batch_list,int(math.ceil(len(batch_list)/float(25)))):
    main_request={'batchRequests':[]}
    for record in chunk:
        subquery_dict={
        "method" : "POST",
        "url" : API_BASE_URL+"/services/data/v39/sobjects/%s/"%(OBJECT),
        "richInput" : record
        }
        
        main_request["batchRequests"].append(subquery_dict)
        
    call=sf_api_call('/services/data/v39.0/composite/batch/', method="post", data=json.dumps(main_request))
"""

main_output_name = get_output_names_for_role('main')[0]
output_dataset =  dataiku.Dataset(main_output_name)
output_dataset.write_with_schema(df)