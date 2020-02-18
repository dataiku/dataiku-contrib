# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *

def batch_geo_parse_regulars(res_parsed,out_cols):
    
    d = pd.DataFrame([res_parsed],columns=out_cols[:-3]).to_dict('record')[0]
    d['block_id'] = d['matched_state_id']+d['matched_county_id']+d['matched_tract_id']+d['matched_block_id']
    d['block_group_id'] = d['block_id'][:12]
    d['tract_id'] = d['block_id'][:11]

    return d