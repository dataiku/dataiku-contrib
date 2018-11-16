# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests, time, json
from datetime import datetime
from dataiku.customrecipe import *
import dataiku_esri_utils
import dataiku_esri_content_utils
from dataiku_esri_utils import recipe_config_get_str_or_none
import common, enrichment

C_DATACOLLECTIONS_SEARCH_ISCASE = False
C_DATACOLLECTIONS_SEARCH_ISREGEX = True

P_USERNAME, P_PASSWORD, P_TOKEN_EXPIRATION, P_BATCH_SIZE_UNIT, P_EPSG_OUT_SR, \
    P_PAUSE, P_SAMPLE = common.read_common_params()

P_COLUMN_OBJECT_ID = get_recipe_config()['column_object_id']
P_COLUMN_LAT = get_recipe_config()['column_lat']
P_COLUMN_LON = get_recipe_config()['column_lon']
P_AREA_TYPE = str(get_recipe_config()['area_type'])
P_BUFFER_UNITS = str(get_recipe_config()['buffer_units'])
P_BUFFER_RADII = int(get_recipe_config()['buffer_radii'])
P_COLUMN_COUNTRY = str(get_recipe_config()['country'])
P_COUNTRY_MODE = get_recipe_config()['country_mode']
P_KEY_COLLECTIONS = get_recipe_config()['key_collections']

P_SPECIFIC_DATACOLLECTION_ID = recipe_config_get_str_or_none('specific_datacollection_id')
P_DATACOLLECTIONS_SEARCH = recipe_config_get_str_or_none('datacollection_search')
P_DERIVATIVE = get_recipe_config()['add_derivative']
P_RETURN_GEOMETRY = get_recipe_config()['return_geometry']

P_OPTION_DATA_AS_TRANSACTIONS = get_recipe_config()['store_enrichment_as_key_value']
P_ACTIVATE_BACKUP = get_recipe_config()['activate_backup']
P_EPSG_IN_SR=  int(get_recipe_config()['in_sr'])

input_name = get_input_names_for_role('input')[0]

# If we have a data catalog, search data collections from it
input_catalog_names =  get_input_names_for_role('datacollections')
df_datacollections = None
if len(input_catalog_names)> 0:
    df_datacollections = dataiku.Dataset(input_catalog_names[0]).get_dataframe()
    print "Input catalog enabled total_coll=%s specific=%s search=%s" % (df_datacollections.shape[0], P_SPECIFIC_DATACOLLECTION_ID, P_DATACOLLECTIONS_SEARCH)

    if P_SPECIFIC_DATACOLLECTION_ID is not None:
        df_datacollections1 = df_datacollections[df_datacollections['collection_id']==P_SPECIFIC_DATACOLLECTION_ID]
        nb_records_df_datacollections1 = df_datacollections1.shape[0]
        print 'info :  Before country filtering nb records in data collections dataset (specific collection) : %s' % (nb_records_df_datacollections1)

    if P_DATACOLLECTIONS_SEARCH is not None:
        df_datacollections2 = df_datacollections[df_datacollections['collection_long_description'].str.contains(P_DATACOLLECTIONS_SEARCH, case=C_DATACOLLECTIONS_SEARCH_ISCASE, regex=C_DATACOLLECTIONS_SEARCH_ISREGEX)]       
        nb_records_df_datacollections2 = df_datacollections2.shape[0]
        print 'info :  Before country filtering nb records in data collections dataset (search collection) : %s' % (nb_records_df_datacollections2)

    if P_SPECIFIC_DATACOLLECTION_ID is not None and  P_DATACOLLECTIONS_SEARCH is not None:
        df_datacollections = pd.concat((df_datacollections1, df_datacollections2), axis=0)
        df_datacollections = df_datacollections.drop_duplicates() #take_last=True
    elif P_SPECIFIC_DATACOLLECTION_ID is not None:
        df_datacollections = df_datacollections1
    elif P_DATACOLLECTIONS_SEARCH is not None:
         df_datacollections = df_datacollections2

# Initialize outputs
output_geocoding_results = get_output_names_for_role('output')[0]
result_dataset = dataiku.Dataset(output_geocoding_results)

output_api_log = get_output_names_for_role('log')
log_api_dataset = None
if len(output_api_log) >0:
    log_api_dataset = dataiku.Dataset(output_api_log[0])

output_metadata = get_output_names_for_role('metadata')[0]
metadata_dataset = None
if len(output_metadata) >0:
    metadata_dataset = dataiku.Dataset(output_metadata)

output_xy_geometry = get_output_names_for_role('geometry')
geometry_dataset = None
if len(output_xy_geometry) >0:
    geometry_dataset = dataiku.Dataset(output_xy_geometry[0])

df_values = pd.DataFrame()
df_metadata = pd.DataFrame()
df_api_log = pd.DataFrame()
df_geometry_result = pd.DataFrame()

(app_token,_) = dataiku_esri_utils.get_token_from_login_password(P_USERNAME,P_PASSWORD,P_TOKEN_EXPIRATION)
dict_esri_coverage_structure = dataiku_esri_utils.get_coverage_dict(P_COUNTRY_MODE)

studyareas_options_dict = {
    "areaType":P_AREA_TYPE
    ,"bufferUnits":P_BUFFER_UNITS
    ,"bufferRadii":[P_BUFFER_RADII]
}

for i,df in enumerate(dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT )):
    print 'Processing this batch id = %s df_shape=%s' % (i, df.shape[0]) 

    df = df[(df[P_COLUMN_LAT].notnull() | df[P_COLUMN_LON].notnull())]
    df = df[-((df[P_COLUMN_LON]==0) & (df[P_COLUMN_LAT]==0))]
    print "After removal of null lat/lon: %s rows remain for this batch" % (df.shape[0])

    if P_SAMPLE >0:
      df = df.head(P_SAMPLE)

    date = datetime.now().isoformat()

    # Make a separate query for each country in the chunk
    # defining the country is mandatory to match the right datacollections. Each country has a specific set of datacollections and fields
    for c in {k: list(v) for k,v in df.sort_values([P_COLUMN_COUNTRY],ascending=[1]).groupby(P_COLUMN_COUNTRY)[P_COLUMN_OBJECT_ID]}:
        print 'Processing this country: %s' % (c)
        try:
            isocode2 = dict_esri_coverage_structure[c]['attributes'][u'isocode2']
            is_country_ok = True
        except:
            is_country_ok = False
            ## be able to track if the issue came from an unsupported country; reported in log
           
        if is_country_ok is True:
            
            dfsub = df[(df[P_COLUMN_COUNTRY]==c)]

            datacollection_list = list()

            # If we have gathered some collection names from the catalog, add them
            if df_datacollections is not None:
                remaining_collections = np.unique(df_datacollections[df_datacollections['country']==isocode2]['collection_id']).tolist()
                datacollection_list.extend(remaining_collections)
                print 'info: number of datacollections for enrichment: %s' % (len(datacollection_list))

            # If we have 0 collection or required by user, add the per-country or global key facts
            if len(datacollection_list) == 0 or P_KEY_COLLECTIONS is True:
                try:
                  datacollection_list = eval(dict_esri_coverage_structure[c]['attributes'][u'datacollections'][u'generic_datacollections'])
                except:
                  datacollection_list = ["KeyGlobalFacts"]

            # We should not have duplicates here, but still check ...
            df_collections_tmp = pd.DataFrame(datacollection_list)
            if df_collections_tmp.shape[1] >1:
                df_collections_tmp = df_collections_tmp.T
            df_collections_tmp.columns = ['datacol']
            df_collections_tmp = df_collections_tmp.drop_duplicates()
            datacollection_list = str(df_collections_tmp['datacol'].tolist())

            print "Requesting data collections: %s" % (datacollection_list)

            # Build the dict of records to submit to the API
            studyareas_list = []
            for row in dfsub.to_dict('records'):
                studyareas_list.append({
                  'geometry':{
                      'x': row[P_COLUMN_LON],
                      'y': row[P_COLUMN_LAT]
                  },
                  'attributes': {'UserObjectID': row[P_COLUMN_OBJECT_ID] }
                })

            source_country_dict = {'sourceCountry': isocode2 }

            custom_params = {
              'studyareas': str(studyareas_list)  
                      , 'UseData': str(source_country_dict)
                      , 'StudyAreasOptions': str(studyareas_options_dict) 
                      , 'dataCollections':datacollection_list 
                      , 'token':app_token
                      , 'forStorage': common.FOR_STORAGE
                      , 'inSR':P_EPSG_IN_SR
                      , 'outSR':P_EPSG_OUT_SR
                      , 'f':'json'
            }

            if P_DERIVATIVE is True:
                custom_params['addDerivativeVariables'] = 'all';
            if P_RETURN_GEOMETRY is True:
                custom_params['returnGeometry'] = True

            r = requests.post('https://geoenrich.arcgis.com/arcgis/rest/services/World/geoenrichmentserver/GeoEnrichment/enrich', params= custom_params)

    
            if r.status_code==200 and not "error" in r.json():
                api_result = r.json()
                
                try:

                    api_message = " / ".join(api_result.get("messages", []))

                    if not "results" in api_result or len(api_result["results"]) == 0:
                        df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, "No API result for these objects",
                                    datacollections = datacollection_list,
                                    country = isocode2)
                    else:
                        results_values = api_result['results'][0][u'value']
                        n_result = len(results_values[u'FeatureSet'][0][u'features'])

                        print "Have %s results in the batch" % (n_result)

                        for ii in range(0,n_result):
                            df_values = enrichment.append_item_features(df_values, results_values, ii, P_OPTION_DATA_AS_TRANSACTIONS)
                            features = results_values[u'FeatureSet'][0][u'features'][ii]['attributes']

                            if P_RETURN_GEOMETRY is True: #== 'true':
                                UserObjectID = features['UserObjectID']

                                geom = results_values[u'FeatureSet'][0][u'features'][ii][u'geometry'] 

                                geom_list =[]
                                for geom_type,v in geom.iteritems():
                                    tupl = (UserObjectID,date,geom_type,P_AREA_TYPE,P_BUFFER_UNITS,[P_BUFFER_RADII],geom[geom_type])
                                    geom_list.append(tupl)

                                df_geometry_result_tmp = pd.DataFrame(geom_list)
                                df_geometry_result_tmp.columns = ['UserObjectID','collected_at','geom_type','areaType','bufferUnits','bufferRadii','the_geom']
                                df_geometry_result = pd.concat((df_geometry_result, df_geometry_result_tmp), axis=0)

                        df_metadata = enrichment.update_batch_metadata(df_metadata, results_values, isocode2)
                        df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date, "Done",
                                datacollections = datacollection_list,
                                country = isocode2)
                        
                except:
                    message = api_result['messages'][0]['description']
                    print message
                    df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date,
                            "Esri Datacollection changed",
                            datacollections = datacollection_list,
                            country = isocode2)
                    
        
            else:
                print "ESRI API failure"
                df_api_log = common.log_api_message(df_api_log, r, i, custom_params, date,
                            "ESRI API failure",
                            datacollections = datacollection_list,
                            country = isocode2)
        else:
            error="Country: %s not supported by the API or Plugin content to be updated" % (c)
            df_api_log = common.log_api_message(df_api_log, '', i, '', date, error) 
        time.sleep(P_PAUSE)

enrichment.write_outputs(
    result_dataset, df_values,
    metadata_dataset, df_metadata,
    geometry_dataset, df_geometry_result,
    log_api_dataset, df_api_log,
    P_ACTIVATE_BACKUP, 'xy',
    P_OPTION_DATA_AS_TRANSACTIONS,
    date,)