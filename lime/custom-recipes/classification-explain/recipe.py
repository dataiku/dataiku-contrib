# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import os
from pprint import pprint
from dkulime.explanation import LimeExplainer

params = get_recipe_config()

MAX_SAMPLES = int(params.get('num_samples'))
if MAX_SAMPLES < 0:
    MAX_SAMPLES = -1
    
NH_SIZE = int(params.get('nh_size'))
if NH_SIZE <= 0:
    raise ValueError('Number of perturbations must be a postive integer')
    
KERNEL_WIDTH = float(params.get('kernel_width'))
RIDGE_ALPHA = float(params.get('alpha_ridge', 1.0))

model_name = get_input_names_for_role('input_model')[0]
model = dataiku.Model(model_name)

train_ds_path = get_input_names_for_role('train')[0]
train_ds = dataiku.Dataset(train_ds_path)
train_df = train_ds.get_dataframe()
test_ds_config = get_input_names_for_role('test')

#Explain train dataset if empty
if len(test_ds_config):
    test_ds_path = test_ds_config[0]
    test_ds = dataiku.Dataset(test_ds_path) 
else:
    test_ds = train_ds
    
test_df = test_ds.get_dataframe()
explanations_ds_path = get_output_names_for_role('explanations')[0]
explanations_ds = dataiku.Dataset(explanations_ds_path)
samples_ds_path = get_output_names_for_role('samples')[0]
samples_ds = dataiku.Dataset(samples_ds_path)

limexp = LimeExplainer(train_df, model, kernel_width=KERNEL_WIDTH, ridge_alpha=RIDGE_ALPHA)

if MAX_SAMPLES < 0:
    to_explain = test_df
else:
    to_explain = test_df.head(MAX_SAMPLES)

explanations_writer = explanations_ds.get_writer()
samples_writer = samples_ds.get_writer()

for idx, (exp_df, instance_df) in enumerate(limexp.iter_explain(test_df, NH_SIZE)):
    if idx == 0:
        explanations_ds.write_schema_from_dataframe(exp_df, True)
        samples_ds.write_schema_from_dataframe(instance_df, True)
    
    explanations_writer.write_dataframe(exp_df)
    samples_writer.write_dataframe(instance_df)

explanations_writer.close()
samples_writer.close()

sys.exit(0)
            