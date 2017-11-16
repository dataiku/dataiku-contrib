# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from dataikuapi import APINodeClient

# Config
api_port       = int(get_recipe_config()['api_port'])
api_service    = get_recipe_config()['api_service']
api_endpoint   = get_recipe_config()['api_endpoint']
api_uri        = get_recipe_config()['api_uri'] + ':' + str(api_port)
should_flatten = get_recipe_config()['should_flatten']
chunksize      = int(get_recipe_config()['chunksize'])

api_node_client = APINodeClient(api_uri, api_service)

# Recipe datasets
input_ds_name = get_input_names_for_role('original_dataset')[0]
input_ds = dataiku.Dataset(input_ds_name)

output_ds_name = get_output_names_for_role('scored_dataset')[0]
output_ds = dataiku.Dataset(output_ds_name)

# Helper function to flatten dictionaries
def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
            
    return dict(items)

# Takes a dataframe, sends it to the API, and returns the properly formatted results
def process_df(dataframe):
    # Preparing the dataframe so that each row is transforwed into a dict
    # See: https://doc.dataiku.com/dss/latest/apinode/user_api.html#dataikuapi.APINodeClient.predict_records
    columns = dataframe.columns.tolist()
    records = map(lambda row: {'features': dict(zip(columns, row))}, dataframe.values)
    preds = api_node_client.predict_records(api_endpoint, records)
    
    results = preds['results']
    
    if should_flatten:
        for result in results:
            flattened_probas = flatten_dict(result['probas'], parent_key='proba')
            result.update(flattened_probas)
            result.pop('probas')
    
    return pd.DataFrame.from_dict(results)


writer = None

try:
    for partial_df in input_ds.iter_dataframes(chunksize=chunksize):
        preds = process_df(partial_df)

        # Reseting index values so that the two dataframes concatenate properly
        partial_df.reset_index(drop=True, inplace=True)
        preds.reset_index(drop=True, inplace=True)
        output = pd.concat([partial_df, preds], axis=1)

        # First loop, we write the schema before creating the dataset writer
        if writer is None:
            output_ds.write_schema_from_dataframe(output)
            writer = output_ds.get_writer()

        writer.write_dataframe(output)
finally:
    if writer is not None:
        writer.close()

