import json
import dataiku
import requests
from dataiku.customrecipe import *
from azure_cognitive_services import *


#==============================================================================
# PLUGIN SETTINGS
#==============================================================================

# Input & output dataset
INPUT_DS_NAME   = get_input_names_for_role('input-dataset')[0]
OUTPUT_DS_NAME  = get_output_names_for_role('output-dataset')[0]

# Recipe settings
API_KEY         = get_recipe_config().get('api-key', None)
AZURE_LOCATION  = get_recipe_config().get('azure-location', None)
TEXT_COLUMN     = get_recipe_config().get('text-column', None)
LANGUAGE_COLUMN = get_recipe_config().get('language-column', '')
OUTPUT_COLUMN   = get_recipe_config().get('output-column', 'entities')
BATCH_SIZE      = get_recipe_config().get('batch-size', None)
READING_LIMIT   = get_recipe_config().get('reading-limit', None)


#==============================================================================
# INPUT AND OUTPUT DATA
#==============================================================================

# Reading in input datasets
in_ds = dataiku.Dataset(INPUT_DS_NAME)
in_sc = in_ds.read_schema()

# Recipe outputs
out_ds = dataiku.Dataset(OUTPUT_DS_NAME)

# Creating output dataset schema
out_sc = in_sc
out_sc.append({'name': OUTPUT_COLUMN,   'type':'string'},)
out_ds.write_schema(out_sc)

# Getting the handle to write output data
writer = out_ds.get_writer()


#==============================================================================
# QUERYING API SERVICE
#==============================================================================

# Building base query URL and headers
endpoint = "https://{}.api.cognitive.microsoft.com/text/analytics/v2.1-preview".format(AZURE_LOCATION)
service  = "/entities"
full_url =  endpoint + service

headers = {
    'Content-Type': 'application/json',
    'Ocp-Apim-Subscription-Key': API_KEY
}

# Actually building and submitting the query
if READING_LIMIT > 0:
    iterator = in_ds.iter_rows(limit=READING_LIMIT, log_every=BATCH_SIZE)
else:
    iterator = in_ds.iter_rows(log_every=BATCH_SIZE)

for group in grouper(BATCH_SIZE, iterator):
    
    # Data to pass to the API
    data = {}
    data["documents"] = []
    for i, record in enumerate(group):
        d = {}
        d["id"] = i
        d["text"] = record[TEXT_COLUMN]
        if len(LANGUAGE_COLUMN) > 0:
            d["language"] = record[LANGUAGE_COLUMN].strip().lower()
        data["documents"].append(d)
    try:
        r = requests.post(full_url, json=data, headers=headers)
    except Exception, e:
        print "[-] Failed to submit query"
        print "[-] {}".format( str(e) )
    
    # Results of the query
    for i, record in enumerate(group):
        o = dict(record)
        try:
            o[OUTPUT_COLUMN] = json.dumps( r.json()["documents"][i]["entities"] )
        except:
            o[OUTPUT_COLUMN] = json.dumps( r.json() )
            
        writer.write_row_dict(o)