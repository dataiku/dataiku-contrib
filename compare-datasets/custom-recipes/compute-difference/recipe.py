# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
import logging

from itertools import islice
from dataiku import pandasutils as pdu
from pandas.api.types import is_numeric_dtype
from dataiku.customrecipe import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

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

if value_columns:
    for value_column in value_columns:
        if (is_numeric_dtype(output[value_column + '_x']) and is_numeric_dtype(output[value_column + '_y'])):
            output[value_column + '_diff'] = output[value_column + '_x'].subtract(output[value_column + '_y']).abs()
            output[value_column + '_isok'] = output[value_column + '_diff'].lt(tolerance)
        else:
            output[value_column + '_isok'] = output[value_column + '_x'].eq(output[value_column + '_y'])

    isok_summary = output[value_columns[0] + '_isok']
    for value_column in islice(value_columns, 1, None):
        isok_summary &= output[value_column + '_isok']
            
    output.insert(loc=0, column='isok_summary', value=isok_summary)


else:
    logging.info("No value columns selected for comparison; only performing a merge")



# Recipe outputs
output_difference.write_with_schema(output)
