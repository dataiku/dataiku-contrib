# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
import time, requests
from datetime import datetime
from dataiku.customrecipe import *
import dataiku_esri_content_utils
import dataiku_esri_utils
from dataiku_esri_utils import recipe_config_get_str_or_none
import common, enrichment

# Read params
P_USERNAME, P_PASSWORD, P_TOKEN_EXPIRATION, P_BATCH_SIZE_UNIT, P_EPSG_OUT_SR, \
    P_PAUSE, P_SAMPLE = common.read_common_params()

P_COLUMN_OBJECT_ID =get_recipe_config()['column_object_id']
P_COLUMN_COUNTRY=get_recipe_config()['country']
P_COUNTRY_MODE=get_recipe_config()['country_mode']
P_AREA_NAME_IN_USER_DATA = get_recipe_config()['column_user_area'] 
P_LAYER_TYPE=get_recipe_config()['layer_type']
P_I_HAVE_A_LAYER= str(recipe_config_get_str_or_none('i_know_the_layer_name'))
P_KEY_COLLECTIONS = get_recipe_config()['key_collections']
P_DERIVATIVE = get_recipe_config()['add_derivative']
P_RETURN_GEOMETRY=get_recipe_config()['return_geometry']
P_OPTION_DATA_AS_TRANSACTIONS=get_recipe_config()['store_enrichment_as_key_value']
P_ACTIVATE_BACKUP=get_recipe_config()['activate_backup']

# Inputs
input_name = get_input_names_for_role('input')[0]
input_datacollections = get_input_names_for_role('datacollections')[0]
df_datacollections = dataiku.Dataset(input_datacollections).get_dataframe()

# Outputs
output_geocoding_results = get_output_names_for_role('output')[0]
result_dataset = dataiku.Dataset(output_geocoding_results)

output_api_log = get_output_names_for_role('log')
log_api_dataset = None
if len(output_api_log) >0:
    log_api_dataset = dataiku.Dataset(output_api_log[0])

output_metadata = get_output_names_for_role('metadata')
metadata_dataset = None
if len(output_metadata) >0:
    metadata_dataset = dataiku.Dataset(output_metadata[0])

output_geometry = get_output_names_for_role('geometry')
geometry_dataset = None
if len(output_geometry) >0:
    geometry_dataset = dataiku.Dataset(output_geometry[0])


df_values = pd.DataFrame()
df_metadata = pd.DataFrame()
df_api_log = pd.DataFrame()
df_geometry_result = pd.DataFrame()

(app_token,_) = dataiku_esri_utils.get_token_from_login_password(P_USERNAME,P_PASSWORD,P_TOKEN_EXPIRATION)
dict_esri_coverage_structure = dataiku_esri_utils.get_coverage_dict(P_COUNTRY_MODE)

for i,df in enumerate(dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT )):

    if P_SAMPLE>0:
        df = df.head(P_SAMPLE)
    print 'Processing batch id = %s (nb_rows=%s)' % (i , df.shape[0])

    df = df[df[P_AREA_NAME_IN_USER_DATA].notnull()]
    nullnb = df[-df[P_AREA_NAME_IN_USER_DATA].notnull()].shape[0]
    print '%s Null rows removed from this batch regarding column = %s' % (nullnb,P_AREA_NAME_IN_USER_DATA)

    date = datetime.now().isoformat()

    df = df.sort([P_COLUMN_COUNTRY],ascending=[1])
    
    
    # Make a separate query for each country in the chunk
    for c in {k: list(v) for k,v in df.groupby(P_COLUMN_COUNTRY)[P_COLUMN_OBJECT_ID]}:
        print 'Processing this country: %s' % (c)
        try:
            isocode2 = dict_esri_coverage_structure[c]['attributes'][u'isocode2']
            is_country_ok = True
        except:
            pass
            ## be able to track if the issue came from an unsupported country; reported in log
           
        if is_country_ok is True:
            # Extract only the records and datacollections of this country
            dfsub = df[(df[P_COLUMN_COUNTRY]==c)]
            df_datacollections_f = df_datacollections[(df_datacollections['country']== isocode2)]

            esri_layer_id = None

            if P_LAYER_TYPE == 'zip|postcode.':
                # Find the id of the zip|postcode layer for that country
                df_layer = df_datacollections_f[df_datacollections_f['layer_name'].str.contains('zip|postcode.', case=False, regex=True)].groupby(['country','collection_id']).last().reset_index()
                if df_layer.shape[0] > 0:
                    esri_layer_id = str(df_layer['layer_id'].tolist()[0])
            else:
                esri_layer_id = P_I_HAVE_A_LAYER

            if esri_layer_id is None:
                df_api_log = common.log_message(df_api_log, i, "No layer for this country")
                continue

            user_area_name_list=[]
            for row in dfsub.to_dict('records'):
                object_id = row[P_COLUMN_OBJECT_ID]
                country =  row[P_COLUMN_COUNTRY]
                user_area_name =  row[P_AREA_NAME_IN_USER_DATA]

                try:
                    user_area_name2 = str(int(user_area_name))
                except:
                    user_area_name2 = user_area_name

                user_area_name_list.append(user_area_name2)

            studyareas_list = str([{"sourceCountry":isocode2,"layer":esri_layer_id,"ids":user_area_name_list}])
            use_data = str({"country":isocode2})

            # For the moment, the list of datacollections is:
            #   - generic_datacollections if available for the country
            #   - Else KeyGlobalFacts
            datacollection_list = []

            if P_KEY_COLLECTIONS is True:
                try:
                    datacollection_list = eval(dict_esri_coverage_structure[c]['attributes'][u'datacollections'][u'generic_datacollections'])
                except:
                    pass

            if len(datacollection_list) ==0:
                datacollection_list = ["KeyGlobalFacts"]

            # Weird data manipulation
            df_collections_tmp = pd.DataFrame(datacollection_list)
            if df_collections_tmp.shape[1] >1:
                df_collections_tmp = df_collections_tmp.T
            df_collections_tmp.columns = ['datacol']
            df_collections_tmp = df_collections_tmp.drop_duplicates()
            datacollection_list = str(df_collections_tmp['datacol'].tolist())

            print "Querying these data collections : %s" % (datacollection_list)

            custom_params = {
                 'StudyAreas': studyareas_list  
                #, 'UseData': use_data # '{"country":"US"}'
                , 'dataCollections':datacollection_list 
                , 'token':app_token
                 , 'forStorage': common.FOR_STORAGE
                 , 'outSR':P_EPSG_OUT_SR
                , 'f':'json'
            }

            if P_DERIVATIVE is True:
                custom_params['addDerivativeVariables'] = 'all';

            if P_RETURN_GEOMETRY is True:
                custom_params["returnGeometry"] = True

            r = requests.post('https://geoenrich.arcgis.com/arcgis/rest/services/World/geoenrichmentserver/GeoEnrichment/enrich', params= custom_params)

            if r.status_code==200 and not "error" in r.json():
                api_result = r.json()

                try:
                    api_message = " / ".join(api_result.get("messages", []))

                    if not "results" in api_result or len(api_result["results"]) == 0:
                        df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, "No API result for these objects")
                    else:
                        results_values = api_result['results'][0][u'value']
                        #print results_values
                        n_result = len(results_values[u'FeatureSet'][0][u'features'])
                        #print "Result is %s " % results_values
                        for ii in range(0,n_result):

                            features = results_values[u'FeatureSet'][0][u'features'][ii]['attributes']
                            df_values = enrichment.append_item_features(df_values, results_values, ii, P_OPTION_DATA_AS_TRANSACTIONS)

                            if P_RETURN_GEOMETRY is True:
                                StdGeographyID = features[u'StdGeographyID'] #: u'33134',
                                StdGeographyLevel = features[u'StdGeographyLevel'] #: u'US.ZIP5'

                                geom = results_values[u'FeatureSet'][0][u'features'][ii][u'geometry'] 

                                geom_list =[]
                                for geom_type,v in geom.iteritems():
                                    tupl = (StdGeographyID,StdGeographyLevel,date,geom[geom_type]) #,geom_type,P_AREA_TYPE,P_BUFFER_UNITS,[P_BUFFER_RADII]
                                    geom_list.append(tupl)

                                df_geometry_result_tmp = pd.DataFrame(geom_list)
                                df_geometry_result_tmp.columns = ['StdGeographyID','StdGeographyLevel','collected_at','the_geom'] #,'geom_type','areaType','bufferUnits','bufferRadii'
                                df_geometry_result = pd.concat((df_geometry_result, df_geometry_result_tmp), axis=0)

                        df_metadata = enrichment.update_batch_metadata(df_metadata, results_values, isocode2)
                        df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, "Done")

                except:
                    try:
                        error_desc = api_result['messages'][0]['description']
                        df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, error_desc)
                        print 'esriJobMessageTypeWarning'

                    except:
                        df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, 'Unknown Error')
                        print 'Unknown Error'


            else:
                print "ESRI API failure"
                df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, "ESRI API failure")
        else:
            df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, "Country not supported by the API: %s") % (c)
        time.sleep(P_PAUSE)

enrichment.write_outputs(
    result_dataset, df_values,
    metadata_dataset, df_metadata,
    geometry_dataset, df_geometry_result,
    log_api_dataset, df_api_log,
    P_ACTIVATE_BACKUP, 'layer',
    P_OPTION_DATA_AS_TRANSACTIONS,date)