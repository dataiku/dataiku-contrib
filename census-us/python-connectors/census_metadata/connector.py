# -*- coding: utf-8 -*-
from dataiku.connector import Connector
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import numpy as np
import os
import census_resources
import common
import census_metadata


class USCensusConnector(Connector):

    def __init__(self, config, plugin_config):

        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        # perform some more initialization
        self.P_CENSUS_CONTENT = self.config.get("param_census_content")


    def get_read_schema(self):
        
        
        all_fields_list = ['name','label','concept','type'] #,'category'
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

        

        P_CENSUS_TYPE = self.P_CENSUS_CONTENT[:3]
        CENSUS_TYPE = str(census_resources.dict_vintage_[self.P_CENSUS_CONTENT[:3]])
        
        
        vint = census_resources.dict_vintage_[P_CENSUS_TYPE][self.P_CENSUS_CONTENT]
        url_metadata = vint['variables_definitions']
        
        if url_metadata.endswith('.json'): 
            m = census_metadata.get_metadata_sources_from_api(url_metadata)
            
        else:
            m = census_metadata.get_metadata_sources(url_metadata)
        
        status = m[0]
        
        if status=='ok':
            metadata_full_df = m[1]
            
            print 'Total number of variables: %s' % (metadata_full_df.shape[0])
            
            for i, line in metadata_full_df.iterrows():
                yield line.to_dict()
        
        else:
            print status

        
            



    
