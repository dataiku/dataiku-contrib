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

def append_item_features(df_values, results_values, ii, P_OPTION_DATA_AS_TRANSACTIONS):
    features = results_values[u'FeatureSet'][0][u'features'][ii]['attributes']

    if P_OPTION_DATA_AS_TRANSACTIONS is True:
        df_values_tmp = pd.DataFrame.from_dict(features, orient='index')
        df_values_tmp= df_values_tmp.reset_index()
        df_values_tmp.columns=['name','value']
    else:
        df_values_tmp = pd.DataFrame.from_dict(features, orient='index').T
    return df_values.append(df_values_tmp)

def update_batch_metadata(df_metadata, results_values, country):
    fd = results_values[u'FeatureSet'][0][u'fields']
    df_fields_definition_tmp_master =pd.DataFrame()
    for fd_tmp in fd:
        df_fields_definition_tmp = pd.DataFrame.from_dict(fd_tmp, orient='index').T
        df_fields_definition_tmp_master = pd.concat((df_fields_definition_tmp_master, df_fields_definition_tmp), axis=0)
        df_fields_definition_tmp_master['Country']=country

    df_metadata = pd.concat((df_metadata, df_fields_definition_tmp_master), axis=0)
    df_metadata = df_metadata[df_metadata['component'].notnull()]
    return df_metadata

def write_outputs(
    result_dataset, df_values,
    metadata_dataset, df_metadata,
    geometry_dataset, df_geometry_result,
    log_api_dataset, df_api_log,
    P_ACTIVATE_BACKUP, backup_basename,
    P_OPTION_DATA_AS_TRANSACTIONS,date
    ):
    # UGLY Temporary
    if P_ACTIVATE_BACKUP is True:
        backup_path = dataiku.get_custom_variables()["dip.home"] + '/tmp/'
        filename = 'dataiku_plugin_esri_' + backup_basename + '_data_backup_' + date + '.csv' 
        f = backup_path + filename
        print 'Exporting backup of your data with (key,value) format: %s' % (P_OPTION_DATA_AS_TRANSACTIONS) 
        df_values.to_csv(f,sep='|',index='none')
        print 'Backup stored into: %s ' % (f)

    result_dataset.write_with_schema(df_values)

    if metadata_dataset is not None and df_metadata.shape[0] > 0:
        print "Writing metdata: %s" % df_metadata
        df_metadata = df_metadata.reset_index()
        df_metadata = df_metadata.drop('index',axis=1)
        df_metadata = df_metadata.drop_duplicates(take_last=True)
        metadata_dataset.write_with_schema(df_metadata)

    if geometry_dataset is not None:
        geometry_dataset.write_with_schema(df_geometry_result)

    if log_api_dataset is not None:
        log_api_dataset.write_with_schema(df_api_log)