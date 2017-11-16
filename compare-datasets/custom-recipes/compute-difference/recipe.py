# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *

# Getting inputs & outputs
input_first_ds_name = get_input_names_for_role('first_dataset')[0]
input_first_ds = dataiku.Dataset(input_first_ds_name)

input_second_ds_name = get_input_names_for_role('second_dataset')[0]
input_second_ds = dataiku.Dataset(input_second_ds_name)

output_difference_name = get_output_names_for_role('difference')[0]
output_difference = dataiku.Dataset(output_difference_name)

# Parameters
key_columns = get_recipe_config()['key_columns']
value_columns = get_recipe_config()['value_columns']
tolerance = float(get_recipe_config()['tolerance'])

# Getting dataframes
input_first_df = input_first_ds.get_dataframe()
input_second_df = input_second_ds.get_dataframe()

# Performing an outer join on the key columns
output = input_first_df.merge(input_second_df, on=key_columns, how='outer')
for value_column in value_columns:  
    output[value_column + '_diff']= output.apply(lambda row: abs(row[value_column + '_x'] - row[value_column + '_y']), axis=1)
    output[value_column + '_isok']= output.apply(lambda row: row[value_column + '_diff'] < tolerance, axis=1)


# Recipe outputs
output_difference.write_with_schema(output)
