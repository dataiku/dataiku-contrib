# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests 
import time
from dataiku.customrecipe import *
import sys


input_name = get_input_names_for_role('input')[0]


# Recipe out
output_e = get_output_names_for_role('output')[0]
py_recipe_output = dataiku.Dataset(output_e)


schema = [{'name':'block_group','type':'string'}
        ,{'name':'block_id','type':'string'}
        ,{'name':'tract_id','type':'string'}
        ,{'name':'county_id','type':'string'}
        ,{'name':'county_name','type':'string'}
        ,{'name':'lat','type':'float'}
        ,{'name':'lon','type':'float'}
        ,{'name':'state_code','type':'string'}
        ,{'name':'state_id','type':'string'}
        ,{'name':'state_name','type':'string'}]


py_recipe_output.write_schema(schema)
writer = py_recipe_output.get_writer()



P_PAUSE = int(get_recipe_config()['param_api_throttle'])
P_LAT = get_recipe_config()['p_col_lat']
P_LON = get_recipe_config()['p_col_lon']
P_BATCH_SIZE_UNIT = int(get_recipe_config()['param_batch_size'])
if P_BATCH_SIZE_UNIT is None:
    P_BATCH_SIZE_UNIT = 50000

b=-1 
for df in dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT ):


    b = b +1
    n_b = b * P_BATCH_SIZE_UNIT 


    df = df[abs(df[P_LAT]>0) | abs(df[P_LON]>0)]

    dfu = df.groupby([P_LAT,P_LON]).count().reset_index()


    n__ = -1
    for v in dfu.to_dict('records'):

        n__ = n__ + 1
        n_record = n_b + n__

        lat = v[P_LAT]
        lon = v[P_LON]
        print '%s - processing: (%s,%s)' % (n_record,lat, lon)
        call = requests.get('http://data.fcc.gov/api/block/find', params={
                'format': 'json',
                'latitude': lat,
                'longitude': lon,
                'showall': 'true'

        })
        
        if call.status_code == 200:
        
            data = call.json()


            try:
                v = data['Block']['FIPS']


                block_id = v
                block_group = v[:12]
                tract_id = v[:11]
                county_id = data['County']['FIPS']
                county_name = data['County']['name']
                state_id = data['State']['FIPS']
                state_code = data['State']['code']
                state_name = data['State']['name']

                d = [block_group,block_id,tract_id,county_id,county_name,lat,lon,state_code,state_id,state_name]

                writer.write_tuple(d)

            except:
                print 'Unable to find these coordinates in the fcc api: lat:%s , lon:%s' % (lat,lon)

        else:
            print 'Failed. API status: %s' % (call.status_code) 
            sys.exit(1)
            
        time.sleep(P_PAUSE)
        
writer.close()