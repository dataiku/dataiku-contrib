# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests
import urllib
import json
import time
from datetime import datetime
from dataiku.customrecipe import *
import dataiku_esri_utils
import common
from dataiku_esri_utils import recipe_config_get_str_or_none

def run_geocoding_recipe(is_detailed):

    P_USERNAME, P_PASSWORD, P_TOKEN_EXPIRATION, P_BATCH_SIZE_UNIT, P_EPSG_OUT_SR, \
        P_PAUSE, P_SAMPLE = common.read_common_params()

    P_COLUMN_OBJECT_ID = get_recipe_config()['column_object_id']
    P_COLUMN_ADDRESS = get_recipe_config()['column_adress']

    # Optional parameters
    P_COLUMN_COUNTRY = recipe_config_get_str_or_none('column_country')
    P_ADRESS_CATEGORY = recipe_config_get_str_or_none('category')

    # Only for detailed mode
    if is_detailed:
        P_COLUMN_CITY = get_recipe_config()['column_city']
        P_COLUMN_POSTAL = recipe_config_get_str_or_none('column_postal')
        P_COLUMN_REGION = recipe_config_get_str_or_none('column_region')

    # Input and outputs
    input_name = get_input_names_for_role('input')[0]

    output_geocoding_results = get_output_names_for_role('results')[0]
    result_dataset = dataiku.Dataset(output_geocoding_results)

    log_api_dataset = None
    if len(get_output_names_for_role('log')) > 0:
        log_api_dataset = dataiku.Dataset( get_output_names_for_role('log')[0])

    (app_token,_) = dataiku_esri_utils.get_token_from_login_password(P_USERNAME,P_PASSWORD,P_TOKEN_EXPIRATION)

    output_df = {
        "value" : pd.DataFrame(),
        "log" : pd.DataFrame()
    }

    for i,df in enumerate(dataiku.Dataset(input_name).iter_dataframes(chunksize= P_BATCH_SIZE_UNIT )):
        print 'Processing batch #%s' % (i)
        if P_SAMPLE > 0:
            df = df.head(P_SAMPLE)

        #'https://developers.arcgis.com/rest/geocode/api-reference/geocoding-service-output.htm#ESRI_SECTION1_42D7D3D0231241E9B656C01438209440'

        def return_geocoder_results(params_dict):
            print "Requesting data as %s" % params_dict
            return requests.post('https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/geocodeAddresses', data = params_dict)

        def get_params_dict(attributes_dict_list, **kwargs):
            params_dict = {
                    'addresses': str({"records" : attributes_dict_list})
                    ,'token':app_token
                    ,'outSR':P_EPSG_OUT_SR
                    ,'f':'json'
                    ,'forStorage': common.FOR_STORAGE
                }

            if P_ADRESS_CATEGORY is not None:
                params['category'] = P_ADRESS_CATEGORY

            for (k, v) in kwargs.items():
                params_dict[k] = v
            return params_dict

        def build_attributes(batch_record):
            attrs = {
                'OBJECTID':  batch_record[P_COLUMN_OBJECT_ID],
                'Address': batch_record[P_COLUMN_ADDRESS]
            }
            if is_detailed:
                attrs["City"] = batch_record[P_COLUMN_CITY]
                attrs["Region"] = '' if P_COLUMN_POSTAL is None else batch_record[P_COLUMN_POSTAL]
                attrs["Postal"] = '' if P_COLUMN_REGION is None else batch_record[P_COLUMN_REGION]

            return {'attributes': attrs }

        def parse_results(api_result):
            n = len(api_result[u'locations'])
            for ii in range(0,n):
                result_dict = api_result[u'locations'][ii][u'attributes']
                UserObjectID = result_dict[u'ResultID']
                result_dict['latestWkid'] = api_result[u'spatialReference']['latestWkid']
                result_dict['wkid'] = api_result[u'spatialReference']['wkid']
                result_dict['collected_at']= datetime.now().isoformat()
                df_value_tmp = pd.DataFrame.from_dict(result_dict, orient='index').T
                output_df["value"] = pd.concat((output_df["value"], df_value_tmp), axis=0)

        if P_COLUMN_COUNTRY is not None:
            print "Using per-country mode"
            # Case 1: country is in the data
            # We'll make a query for each country + a generic query for lines without country data

            # Build a dataframe of the rows where country is filled
            df_nn = df[df[P_COLUMN_COUNTRY].notnull()]
            nb_records_df_nn = df_nn.shape[0]
            print 'Processing data to geocode: %s adresses in this batch with a country value...' % (nb_records_df_nn)

            df_nn = df_nn.sort([P_COLUMN_COUNTRY],ascending=[1])

            dicz = {k: list(v) for k,v in df_nn.groupby(P_COLUMN_COUNTRY)[P_COLUMN_OBJECT_ID]}

            # Iterate on all countries present in the chunk
            for c in dicz:
                dfsub_nn = df_nn[(df_nn[P_COLUMN_COUNTRY]==c)]
                assert dfsub_nn.shape[0] > 0

                attributes_dict_list = []
                for batch_record in dfsub_nn.to_dict('records'):
                    attributes_dict_list.append(build_attributes(batch_record))
                params_dict = get_params_dict(attributes_dict_list, sourceCountry = c)

                # Send the query
                query_at = datetime.now().isoformat()
                api_resp = return_geocoder_results(params_dict)

                if api_resp.status_code == 200 and not "error" in api_resp.json():
                    parse_results(api_resp.json())

                output_df["log"] = common.log_api_message(output_df["log"], api_resp, i, params_dict, query_at,
                         "country-col-country", country=c)

            # Make a final query with the lines without country specified
            df_null = df[-df[P_COLUMN_COUNTRY].notnull()]
            nb_records_df_null = df_null.shape[0]
            print 'Processing data to geocode: %s adresses in this batch without a country value...' % (nb_records_df_null)

            if nb_records_df_null > 0:
                df_null = df_null.sort([P_COLUMN_COUNTRY],ascending=[1])

                attributes_dict_list_null = []
                for batch_record_null in df_null.to_dict('records'):
                    attributes_dict_list_null.append(build_attributes(batch_record))
                params_dict_null = get_params_dict(attributes_dict_list_null)

                # Send the query
                query_at = datetime.now().isoformat()
                api_resp = return_geocoder_results(params_dict_null)

                if api_resp.status_code == 200  and not "error" in api_resp.json():
                    parse_results(api_resp.json())

                output_df["log"] = common.log_api_message(output_df["log"], api_resp, i, params_dict, query_at,
                         "country-col-no-country")

        else:
            print "Using auto-country mode"
            # Case 2: the country must be guessed

            attributes_dict_list = []
            for batch_record in df.to_dict('records'):
                attributes_dict_list.append(build_attributes(batch_record))
            params_dict = get_params_dict(attributes_dict_list)

            # Send the query
            query_at = datetime.now().isoformat()
            api_resp = return_geocoder_results(params_dict)

            if api_resp.status_code == 200 and not "error" in api_resp.json():
                parse_results(api_resp.json())

            output_df["log"] = common.log_api_message(output_df["log"], api_resp, i, params_dict, query_at,
                         "country-nocol")

        # Wait before the next batch
        time.sleep(P_PAUSE)

    # Flush results
    result_dataset.write_with_schema(output_df["value"])
    if log_api_dataset is not None:
        log_api_dataset.write_with_schema(output_df["log"])