import json
import pandas as pd
from dataiku.customrecipe import *

# ESRI :If the data is being stored,
# the terms of use for the GeoEnrichment service require that you specify the forStorage parameter to true.
# Only set this to False for development purposes
FOR_STORAGE = True

def read_common_params():
    P_USERNAME = get_recipe_config()['username']
    P_PASSWORD = get_recipe_config()['password']
    P_TOKEN_EXPIRATION = int(get_recipe_config()['token_expiration'])
    P_BATCH_SIZE_UNIT = int(get_recipe_config()['batch_size_unit'])
    P_EPSG_OUT_SR=int(get_recipe_config()['out_sr'])
    P_PAUSE=int(get_recipe_config()['api_throttle'])
    P_SAMPLE=int(get_recipe_config()['sample'])

    return (P_USERNAME, P_PASSWORD, P_TOKEN_EXPIRATION,
            P_BATCH_SIZE_UNIT, P_EPSG_OUT_SR, P_PAUSE, P_SAMPLE)

def make_api_log_message(request_resp, batch_id, input_dict, query_at, dku_message, **kwargs):
    json_resp = {}
    try:
        json_resp = request_resp.json()
        sc = request_resp.status_code
    except:
        sc =''

    dic = {
        'http_code':  sc,
        'api_error' : json.dumps(json_resp.get("error", {"details" : "unknown"})),
        'dku_message' : dku_message,
        'batch_id' : batch_id,
        'params_dict' : input_dict,
        'query_at' : query_at
    }
    for (k, v) in kwargs.items():
        dic[k] = v
    return dic

def log_api_message(df_api_log, request_resp, batch_id, input_dict, query_at, dku_message, **kwargs):
    api_failure = make_api_log_message(request_resp, batch_id, input_dict, query_at, dku_message, **kwargs)
    print "API message: %s" % api_failure
    df_api_log_tmp = pd.DataFrame([api_failure])
    return df_api_log.append(df_api_log_tmp)

def make_log_message(batch_id, dku_message, **kwargs):
    dic = {
        'dku_message' : dku_message,
        'batch_id' : batch_id,
    }
    for (k, v) in kwargs.items():
        dic[k] = v
    return dic

def log_message(df_api_log, batch_id, dku_message, **kwargs):
    api_failure = make_log_message(batch_id, dku_message, **kwargs)
    print "Generic message: %s" % api_failure
    df_api_log_tmp = pd.DataFrame([api_failure])
    return df_api_log.append(df_api_log_tmp)