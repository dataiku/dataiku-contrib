# mstr initialization
username = 'PARAM1' # ex: hchadeisson
password = 'PARAM2' # ex: password
base_url = 'PARAM3' # ex: https://env-112094.customer.cloud.microstrategy.com/MicroStrategyLibrary/api
project_name = 'PARAM4' # ex: MicroStrategy Tutorial
dataset_id = 'PARAM5'

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from mstrio import microstrategy # MicroStrategy library for sending data to in-memory cubes
from dataiku import pandasutils as pdu

# Connect to MicroStrategy
conn = microstrategy.Connection(base_url=base_url, username=username, password=password, project_name=project_name)
conn.connect()

# Get dataset from MicroStrategy
mstr_dataset_df = conn.get_cube(cube_id=dataset_id)