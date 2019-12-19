# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests 
#import time
from dataiku.customrecipe import *
import sys
import re
import geocoder_utils
import common
import os


logging.info('1/6 Creating base folder...' )
path_datadir_tmp = dataiku.get_custom_variables()["dip.home"] + '/tmp/'
P_CENSUS_CONTENT = 'geocoder'
FOLDER_NAME = 'tmp_census_us_'+ P_CENSUS_CONTENT 
common.create_folder(path_datadir_tmp,FOLDER_NAME,True)  

input_name = get_input_names_for_role('input')[0]

output_ = get_output_names_for_role('output')[0]
output_dataset = dataiku.Dataset(output_)

P_COL_STREET = get_recipe_config()['p_col_street']
P_COL_CITY = get_recipe_config()['p_col_city']
P_COL_STATE = get_recipe_config()['p_col_state']
P_COL_ZIPCODE = get_recipe_config()['p_col_zipcode']

P_BENCHMARK = get_recipe_config()['p_benchmark']
P_VINTAGE = get_recipe_config()['p_vintage']


if P_BENCHMARK=="9":
    P_VINTAGE ="910"

logging.info('2/6 Parameters:')
logging.info('[+] BENCHMARK = {} ; VINTAGE = {} '.format(P_BENCHMARK,P_VINTAGE))
    
P_BATCH_SIZE_UNIT = int(get_recipe_config()['param_batch_size'])
if P_BATCH_SIZE_UNIT is None:
    P_BATCH_SIZE_UNIT = 5000
    

id_column = get_recipe_config()['p_col_id_column'] 
id_as_int = get_recipe_config()['param_id_as_int'] 

P_KEEP_NON_MATCHING = get_recipe_config()['param_keep_non_matching']
#P_RETRY_TIE  = True #get_recipe_config()['param_retry_tie'] ### Potential optimization by resubmitting the ties.

in_cols = [id_column,P_COL_STREET,P_COL_CITY,P_COL_STATE,P_COL_ZIPCODE]
    
if id_as_int:
    id_type='int'
else:
    id_type='string'

    
logging.info('3/6 Input columns:')
logging.info(in_cols)

schema = [{'name':id_column,'type':id_type}
        ,{'name':'street','type':'string'}
        ,{'name':'city','type':'string'}
        ,{'name':'state','type':'string'}
        ,{'name':'zipcode','type':'string'}
        ,{'name':'match','type':'string'}
        ,{'name':'match_quality','type':'string'}
        ,{'name':'matched_address','type':'string'}
        ,{'name':'matched_state','type':'string'}
        ,{'name':'matched_city','type':'string'}
        ,{'name':'matched_zipcode','type':'string'}
        ,{'name':'matched_longitude','type':'string'}
        ,{'name':'matched_latitude','type':'string'}
        ,{'name':'matched_tigerLineId','type':'string'}
        ,{'name':'matched_side','type':'string'}
        ,{'name':'matched_state_id','type':'string'}
        ,{'name':'matched_county_id','type':'string'}
        ,{'name':'matched_tract_id','type':'string'}
        ,{'name':'matched_block_id','type':'string'}
  
        ,{'name':'tract_id','type':'string'} 
        ,{'name':'block_group_id','type':'string'}  
        ,{'name':'block_id','type':'string'}]  

out_cols=[x['name'] for x in schema]
  
logging.info('4/6 Writing schema...')
output_dataset.write_schema(schema)

logging.info('5/6 Starting Batch...:') 
batch_list_ok = []
b=-1 
with output_dataset.get_writer() as writer:
    for df in dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT , columns = in_cols):
        
        b = b +1
        
        logging.info('Processing batch: %s' % (b))
        
        df = df[df[P_COL_STREET]<>'']
        
        file_full_path = path_datadir_tmp + '/' + FOLDER_NAME + '/' + 'census_geocode_adresses_' + str(b) + '.csv' 
        
        df.to_csv(file_full_path,sep=',',index=None,header=None)
        
        url = 'https://geocoding.geo.census.gov/geocoder/geographies/addressbatch?form'
        payload = {'benchmark':P_BENCHMARK,'vintage':P_VINTAGE,'layers':14}
        files = {'addressFile': (file_full_path, open(file_full_path, 'rb'), 'text/csv')}
        
        try:
        
            batch = requests.post(url, files=files, data = payload)

            if batch.status_code == 200:

                results = str(batch.text)
                results = re.sub('"','',results)
                results = results.split('\n')

                for i,result in enumerate(results[:-1]):
                    res_parsed = results[i].split(',')
                                        
                    try:
                        idx = res_parsed.index('Match')

                        if idx==6:
                            res_parsed[1] = res_parsed[1] + res_parsed[2]
                            del res_parsed[2]
                            
                            d=geocoder_utils.batch_geo_parse_regulars(res_parsed,out_cols) 
                            
                        elif idx==4:
                            ok4 = res_parsed[4]=='Match'
                            res_parsed.insert(3,'-')
                            
                            d=geocoder_utils.batch_geo_parse_regulars(res_parsed,out_cols) 

                        
                        elif idx==5:
                            d=geocoder_utils.batch_geo_parse_regulars(res_parsed,out_cols) 
                            
                                                        
                        else:
                            
                            d={}
                            d[id_column]=res_parsed[0]
                            for k in out_cols[1:]:
                                d[k]='' 
                            d['match']='Matched parsing required'
                            d['match_quality']=res_parsed

                        writer.write_row_dict(d)
                            
                        
                    except:

                        if P_KEEP_NON_MATCHING is True:
                            if len(res_parsed)==6:
                                d = pd.DataFrame([res_parsed],columns=out_cols[:6]).to_dict('record')[0]
                                
                                
                            elif len(res_parsed)==7:
                                res_parsed[1] = res_parsed[1] + res_parsed[2]
                                del res_parsed[2]
                                d = pd.DataFrame([res_parsed],columns=out_cols[:6]).to_dict('record')[0]


                            else:
                                d={}
                                d[id_column]=res_parsed[0]
                                for k in out_cols[1:]:
                                    d[k]='' 
                                d['match']='Custom parsing required'
                                d['match_quality']=res_parsed

                                                                
                            writer.write_row_dict(d)
                        


            else:
                logging.info("[Warning] : API returns this status: {}".format(s.status_code))
        
        #except MaxRetryError as maxerror:
            #print("Max Retries Error:", maxerror)
        except requests.exceptions.HTTPError as Herr:
            logging.info("Http Error:", Herr)
        except requests.exceptions.ConnectionError as errc:
            logging.info("Error Connecting:", errc)
        except requests.exceptions.Timeout as errt:
            logging.info("Timeout Error:", errt)
        except requests.exceptions.RequestException as err:
            logging.info("Something Else", err)
            
        batch_list_ok.append(b)
        
## DEL ALL
logging.info('6/6 Dropping intermediate files...:' )
cmd = "rm -rf %s" % (path_datadir_tmp + '/' +FOLDER_NAME)
os.system(cmd)



            


