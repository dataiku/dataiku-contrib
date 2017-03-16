# -*- coding: utf-8 -*-
from dataiku.connector import Connector
import census_resources
import pandas as pd


class MyConnector(Connector):

    def __init__(self, config, plugin_config):

        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.t__ = census_resources.get_dict_ref()

        
    def get_read_schema(self):
   
        all_fields_list = self.t__.keys()
        
        l=[]
        for field in all_fields_list:
            d={}
            d['name']=field
            d['type']='string'
            l.append(d)

        d_={"columns": l} 
        
        return d_

    
    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        
        dft__ = pd.DataFrame(self.t__)
        
        for i, line in dft__.iterrows():
            yield line.to_dict()


    
