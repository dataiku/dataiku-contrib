# coding: utf-8
import pandas as pd
from datetime import datetime
from dataiku.connector import *
import dataiku
import dataiku_esri_content_utils

class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)

    def get_read_schema(self):
        return {"columns" : [
                                   {"name": "comment", "type" : "string"}
                                 , {"name" :"country", "type" : "string"}
                                 , {"name" :"esri_content_as_of", "type" : "string"}
                                 , {"name" :"isocode3", "type" : "string"}
                                 , {"name" :"isocode2", "type" : "string"}
                                 , {"name" :"generic_datacollections", "type" : "string"}
                            ]}

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        d = dataiku_esri_content_utils.get_esri_coverage()
        df = pd.DataFrame(d)
        df['dataset_created_by_user_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for rec in df.to_dict('records'):
            yield rec
