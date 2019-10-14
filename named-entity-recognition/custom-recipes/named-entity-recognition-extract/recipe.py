# -*- coding: utf-8 -*-
import dataiku
import pandas as pd
from dataiku.customrecipe import *

import warnings

warnings.filterwarnings(action='ignore')

#############################
# Logging Settings
#############################

import logging

FORMAT = '[NER RECIPE] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#############################
# Input & Output datasets
#############################

input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

output_dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(output_dataset_name)

input_df = input_dataset.get_dataframe()

#############################
# Recipe Parameters
#############################

recipe_config = get_recipe_config()

text_column_name = recipe_config.get('text_column_name', None)
if text_column_name == None:
    raise ValueError("You did not choose a text column.")

advanced_settings = recipe_config.get('advanced_settings', False)
if advanced_settings:
    output_single_json = recipe_config.get('output_single_json', False)
    ner_model = recipe_config.get('ner_model', 'spacy')
else:
    output_single_json = False
    ner_model = 'spacy'

if ner_model == 'spacy':
    from ner_utils_spacy import extract_entities
else:
    from ner_utils_flair import extract_entities

#############################
# Main Loop
#############################

CHUNK_SIZE = 100
n_lines = 0
logger.info("Started chunk-processing of input Dataset.")
for chunk_idx, df in enumerate(input_dataset.iter_dataframes(chunksize=CHUNK_SIZE)):
    # Process chunk
    out_df = extract_entities(df[text_column_name].fillna(" "), format=output_single_json)
    df = df.reset_index(drop=True)
    out_df = out_df.reset_index(drop=True)
    out_df = df.merge(out_df, left_index=True, right_index=True)

    # Append dataframe to output Dataset
    if chunk_idx == 0:
        output_dataset.write_schema_from_dataframe(out_df)
        writer = output_dataset.get_writer()
        writer.write_dataframe(out_df)
    else:
        writer.write_dataframe(out_df)

    n_lines += len(df)
    logger.info("Finished processing {} lines".format(n_lines))

writer.close()
