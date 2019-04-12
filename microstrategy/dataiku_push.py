# mstr initialization
username = 'PARAM1' # ex: hchadeisson
password = 'PARAM2' # ex: password
base_url = 'PARAM3' # ex: https://env-112094.customer.cloud.microstrategy.com/MicroStrategyLibrary/api
project_name = 'PARAM4' # ex: MicroStrategy Tutorial

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from mstrio import microstrategy # MicroStrategy library for sending data to in-memory cubes
from dataiku import pandasutils as pdu

# Connect to MicroStrategy
conn = microstrategy.Connection(base_url=base_url, username=username, password=password, project_name=project_name)
conn.connect()

# Read recipe inputs
revenue_prediction = dataiku.Dataset("revenue_prediction")
revenue_prediction_df = revenue_prediction.get_dataframe()

# Send Data to MicroStrategy
newDatasetId, newTableId = conn.create_dataset(data_frame=revenue_prediction_df, dataset_name="dataiku_prediction", table_name="dataiku_prediction")