# coding=utf-8

u"""Map constants to user meaning

This plugin was developped by the Ministère de l’Intérieur (French ministry of interior)
in the context of the program Entrepreneurs d’Intérêt Général 2017
"""

from concurrent import futures
import dataiku
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config
import json
import pandas as pd

# We read the addresses from the input dataset
# And write the coordinates in the output dataset
input_name = get_input_names_for_role('input')[0]
input_dataset = dataiku.Dataset(input_name)

output_name = get_output_names_for_role('output')[0]
output_dataset = dataiku.Dataset(output_name)

meanings = get_recipe_config()['meanings'].encode('utf8')
meanings = json.loads(meanings)

df = input_dataset.get_dataframe(infer_with_pandas=False)

for column, mappings in meanings.iteritems():
    df[column] = df[column].map(mappings)

output_dataset.write_with_schema(df)
