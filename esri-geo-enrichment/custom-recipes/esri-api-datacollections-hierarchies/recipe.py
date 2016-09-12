# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
import time
import json
import requests
from datetime import datetime
from dataiku.customrecipe import *
import dataiku_esri_content_utils
import dataiku_esri_utils
from dataiku_esri_utils import recipe_config_get_str_or_none
import common, enrichment

P_USERNAME = get_recipe_config()['username']
P_PASSWORD = get_recipe_config()['password']
P_TOKEN_EXPIRATION = int(get_recipe_config()["token_expiration"])
P_COLUMN_COUNTRY = recipe_config_get_str_or_none("country")
P_USER_COUNTRY_LIST = recipe_config_get_str_or_none("user_country_list")

(app_token,_) = dataiku_esri_utils.get_token_from_login_password(P_USERNAME,P_PASSWORD,P_TOKEN_EXPIRATION)

def get_layer_name(country_name,app_token):
    call_url =  'https://geoenrich.arcgis.com/arcgis/rest/services/World/geoenrichmentserver/Geoenrichment/StandardGeographyLevels/' + country_name # ex: 'The Former Yugoslav Republic of Macedonia'   or ...           
    return requests.get(call_url, params={
        'f': 'json'
        ,'token': app_token
    }).json()

def get_datacollections(country_name,app_token):
    call_url = 'https://geoenrich.arcgis.com/arcgis/rest/services/World/geoenrichmentserver/Geoenrichment/dataCollections/' + country_name
    custom_params = { 'token':app_token
                    , 'forStorage':common.FOR_STORAGE ## true
                    , 'f':'json'}
    r = requests.post(call_url, params= custom_params)
    return r.json()

# Open output handle
output_results = get_output_names_for_role('output')[0]
result_dataset = dataiku.Dataset(output_results)

# Get the input list of countries
try:
    input_name = get_input_names_for_role('input_countries')[0]
    df  = dataiku.Dataset(input_name).get_dataframe()
    is_input = True
except:
    is_input = False

if is_input is True:
        if P_COLUMN_COUNTRY is not None:
            df = df[df[P_COLUMN_COUNTRY].notnull()]
            country_list = df[P_COLUMN_COUNTRY].drop_duplicates().values.tolist()
        else:
            raise ValueError("The country column parameter is required when using an input dataset")

else:
    try:
        country_list = eval(P_USER_COUNTRY_LIST)
        
    except:
        raise ValueError("You have an issue in your country list")


df_main = pd.DataFrame()


'''
input_name = get_input_names_for_role('input_countries')[0]
if len(inputs) >0:
    input_name =inputs[0]
    df  = dataiku.Dataset(input_name).get_dataframe()
    if P_COLUMN_COUNTRY is None:
        raise ValueError("The country column parameter is required when using an input dataset")
    df = df[df[P_COLUMN_COUNTRY].notnull()]
    country_list = df[P_COLUMN_COUNTRY].drop_duplicates().values.tolist()
else:
    if P_COLUMN_COUNTRY is not None:
        raise ValueError("No input countries dataset, you need to specify a fixed list of countries")
    country_list = json.loads(P_USER_COUNTRY_LIST)

df_main = pd.DataFrame()
'''

# Main work loop
for c in country_list:
    print 'Processing this country: %s' % (c)
    api_result = get_datacollections(c,app_token)

    if not "DataCollections" in api_result:
        raise ValueError("No DataCollections returned for country %s: got answer=%s"% (c, api_result))

    t = {'collection_id':[], 'country':[],'collection_hierarchie':[] ,'collection_long_description':[],'collection_keywords':[] }
    for i in range(0,len(api_result[u'DataCollections'])):
        collection_id = api_result[u'DataCollections'][i][u'dataCollectionID']
        collection_hierarchie = api_result[u'DataCollections'][i]['metadata'][u'hierarchies'] #.split(',')[0]

        t['collection_id'].append(collection_id)
        t['country'].append(c)
        t['collection_hierarchie'].append(collection_hierarchie)
        
        
        try:
            collection_longdescription = api_result[u'DataCollections'][i]['metadata'][u'longDescription']
            t['collection_long_description'].append(collection_longdescription)
        except:
            t['collection_long_description'].append('')
        
        try:
            collection_kw = api_result[u'DataCollections'][i]['metadata'][u'keywords']
            t['collection_keywords'].append(collection_kw)
        except:
            t['collection_keywords'].append('')
        '''
        collection_longdescription = api_result[u'DataCollections'][i]['metadata'].get('longDescription', None)
        collection_kw = api_result[u'DataCollections'][i]['metadata'].get('keywords', None)
        '''
        
    df_collections = pd.DataFrame(t)

    data_layer = get_layer_name(c,app_token) #dataiku_esri_utils.

    df_layer=pd.DataFrame()

    try:
        hn = len(data_layer[u'geographyLevels'][0]['hierarchies'])
        for h in range(0,hn):
            layer_n = len(data_layer[u'geographyLevels'][0]['hierarchies'][h][u'levels'])

            for iln in range(0,layer_n):
                esri_dataset = data_layer[u'geographyLevels'][0]['hierarchies'][h][u'ID']
                layer_name = data_layer[u'geographyLevels'][0]['hierarchies'][h][u'levels'][iln][u'name']
                layer_id = data_layer[u'geographyLevels'][0]['hierarchies'][h][u'levels'][iln][u'id']

                layer_tmp = [c,esri_dataset,layer_id,layer_name]
                df_layer_tmp = pd.DataFrame(layer_tmp).T
                df_layer_tmp.columns= ['country','esri_dataset','layer_id', 'layer_name']
                df_layer = pd.concat((df_layer, df_layer_tmp), axis=0)
    except:
        df_layer_tmp=pd.DataFrame({'country':[c],'esri_dataset':[''],'layer_id':[''], 'layer_name':['']})
        df_layer = pd.concat((df_layer, df_layer_tmp), axis=0)
        

    df_all = pd.merge(left=df_layer,right=df_collections, how='left' , left_on=['country','esri_dataset'], right_on=['country','collection_hierarchie'])
    df_main = pd.concat((df_main, df_all), axis=0)

result_dataset.write_with_schema(df_main)