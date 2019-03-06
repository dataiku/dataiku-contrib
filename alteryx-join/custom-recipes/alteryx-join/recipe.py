import os
import json
import dataiku
import pandas as pd
from dataiku.customrecipe import *
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,  # avoid getting log from 3rd party module
                    format='alteryx-join plugin %(levelname)s - %(message)s')



#==============================================================================
# IOs
#==============================================================================

first_dataset_name = get_input_names_for_role('first-dataset')[0]
first_dataset = dataiku.Dataset(first_dataset_name) 

second_dataset_name = get_input_names_for_role('second-dataset')[0]
second_dataset = dataiku.Dataset(second_dataset_name)

output_dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(output_dataset_name)

#==============================================================================
# Config
#==============================================================================


#==============================================================================
# Run
#==============================================================================



        
#==============================================================================
# OUTPUT
#==============================================================================

ags_img = dataiku.Dataset(OUTPUT_DS_NAME)
ags_img.write_with_schema( pd.DataFrame(o) )