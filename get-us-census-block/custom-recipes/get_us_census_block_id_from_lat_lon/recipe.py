# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests 
import time
from dataiku.customrecipe import *
import sys

print ('## Running Plugin v0.2.6 ##')

input_name = get_input_names_for_role('input')[0]


# Recipe out

output_ = get_output_names_for_role('output')[0]
output_dataset = dataiku.Dataset(output_)

schema = [{'name':'block_group','type':'string'}
        ,{'name':'block_id','type':'string'}
        ,{'name':'tract_id','type':'string'}
        ,{'name':'county_id','type':'string'}
        ,{'name':'county_name','type':'string'}
        ,{'name':'lat','type':'string'}
        ,{'name':'lon','type':'string'}
        ,{'name':'state_code','type':'string'}
        ,{'name':'state_id','type':'string'}
        ,{'name':'state_name','type':'string'}]


#P_ID = get_recipe_config()['param_id']
#P_ID_MIN = get_recipe_config()['param_id_min']
#P_ID_MAX = get_recipe_config()['param_id_max']
P_PAUSE = int(get_recipe_config()['param_api_throttle'])
P_LAT = get_recipe_config()['p_col_lat']
P_LON = get_recipe_config()['p_col_lon']
P_BATCH_SIZE_UNIT = int(get_recipe_config()['param_batch_size'])
if P_BATCH_SIZE_UNIT is None:
    P_BATCH_SIZE_UNIT = 50000
    
strategy = get_recipe_config()['param_strategy']

    
output_bbox= get_recipe_config()['param_output_bbox']
if output_bbox is True:
    schema.append({'name':'bbox','type':'string'})

if get_recipe_config().get('p_id_column', None) is not None:
    use_column_id=True
    id_column = get_recipe_config().get('p_id_column', None)
    id_as_int = get_recipe_config().get('param_id_as_int', None)
    
    if id_as_int:
        schema.append({'name':id_column,'type':'int'})
    else:
        schema.append({'name':id_column,'type':'string'})
else:
    use_column_id=False


output_dataset.write_schema(schema)


b=-1 
with output_dataset.get_writer() as writer:
    for df in dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT ):

        #if P_ID is not None:
            #df = df[(df[P_ID]>=P_ID_MIN) & (df[P_ID]<=P_ID_MAX)]

        b = b +1
        n_b = b * P_BATCH_SIZE_UNIT 


        df = df[abs(df[P_LAT]>0) | abs(df[P_LON]>0)]

        if strategy =='make_unique':
            dfu = df.groupby([P_LAT,P_LON]).count().reset_index()
        else:
            dfu = df.copy()


        n__ = -1
        for v in dfu.to_dict('records'):

            n__ = n__ + 1
            n_record = n_b + n__

            lat = v[P_LAT]
            lon = v[P_LON]
            
            if use_column_id:
                id_ = v[id_column]
            
            print '%s - processing: (%s,%s)' % (n_record,lat, lon)
            call = requests.get('https://geo.fcc.gov/api/census/block/find', params={
                    'format': 'json',
                    'latitude': lat,
                    'longitude': lon,
                    'showall': 'true'

            },verify=False)

            if call.status_code == 200:

                data = call.json()


                try:
                    v = data['Block']['FIPS']

                    d={}

                    d['block_group'] = v[:12]
                    d['block_id'] = v
                    d['tract_id'] = v[:11]
                    d['county_id'] = data['County']['FIPS']
                    d['county_name'] = data['County']['name']
                    d['lat'] = lat
                    d['lon'] = lon
                    d['state_code'] = data['State']['code']
                    d['state_id'] = data['State']['FIPS']
                    d['state_name'] = data['State']['name']

                    col_list_=['block_group','block_id','tract_id','county_id'
                               ,'county_name','lat','lon','state_code','state_id','state_name']

                    if output_bbox is True:
                        d['bbox'] = data['Block'][u'bbox']
                        ####col_list_ = col_list_ + ['bbox']
                        
                        
                    if use_column_id is True:
                        if id_as_int:
                            d[id_column]=int(id_)
                        else:
                            d[id_column]=id_


                    writer.write_row_dict(d)



                except:
                    print 'Unable to find these coordinates in the fcc api: lat:%s , lon:%s' % (lat,lon)

            else:
                print 'Failed. API status: %s' % (call.status_code) 
                print 'The plugin will write the output dataset where the process stopped. You should probably consider filtering your input dataset where the plugin stopped and select the append mode for the input/output panel.'
                sys.exit(1)

            time.sleep(P_PAUSE)



