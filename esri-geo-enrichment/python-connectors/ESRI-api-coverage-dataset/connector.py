# coding: utf-8
import pandas as pd
from datetime import datetime
from dataiku.connector import *
import dataiku
import dataiku_esri_content_utils

class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor.
        The static configuration parameters set up by the developer in the optional
        file settings.json at the root of the plugin directory are passed as a json
        object 'plugin_config' to the constructor
        """
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        # perform some more initialization
        #self.theparam1 = self.config.get("param_client_id", "000")

    def get_read_schema(self):
        #"""
        #Returns the schema that this connector generates when returning rows.

        #The returned schema may be None if the schema is not known in advance.
        #In that case, the dataset schema will be infered from the first rows.

        #If you do provide a schema here, all columns defined in the schema
        #will always be present in the output (with None value),
        #even if you don't provide a value in generate_rows

        #The schema must be a dict, with a single key: "columns", containing an array of
        #{'name':name, 'type' : type}.

        #Example:
        return {"columns" : [ 
                                {"name": "comment", "type" : "string"}
                                , {"name" :"country", "type" : "string"}
                                , {"name" :"esri_content_as_of", "type" : "string"}
                                , {"name" :"isocode3", "type" : "string"}
                                , {"name" :"isocode2", "type" : "string"}
                                , {"name" :"generic_datacollections", "type" : "string"}
                                #, {"name" :"local_datacollections", "type" : "string"}
                                #, {"name" :"landscape_datacollections", "type" : "string"}

                                
                                ]}

        #comment
        #country
        #esri_content_as_of
        #isocode3
        #isocode2
        #generic_datacollections
        #Supported types are: string, int, bigint, float, double, date, boolean
        #"""

        ## In this example, we don't specify a schema here, so DSS will infer the schema
        ## from the columns actually returned by the generate_rows method
        #return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        d = dataiku_esri_content_utils.get_esri_coverage()
        df = pd.DataFrame(d)
        df['dataset_created_by_user_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        for rec in df.to_dict('records'):
            yield rec


    #def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                        #partition_id=None):
        #"""
        #Returns a writer object to write in the dataset (or in a partition).

        #The dataset_schema given here will match the the rows given to the writer below.

        #Note: the writer is responsible for clearing the partition, if relevant.
        #"""
        #raise Exception("Unimplemented")


    #def get_partitioning(self):
        #"""
        #Return the partitioning schema that the connector defines.
        #"""
        ##raise Exception("Unimplemented")


    #def list_partitions(self, partitioning):
        #"""Return the list of partitions for the partitioning scheme
        #passed as parameter"""
        #return []


    #def partition_exists(self, partitioning, partition_id):
        #"""Return whether the partition passed as parameter exists

        #Implementation is only required if the corresponding flag is set to True
        #in the connector definition
        #"""
        #raise Exception("unimplemented")


    #def get_records_count(self, partitioning=None, partition_id=None):
        #"""
        #Returns the count of records for the dataset (or a partition).

        #Implementation is only required if the corresponding flag is set to True
        #in the connector definition
        #"""
        #raise Exception("unimplemented")


#class CustomDatasetWriter(object):
    #def __init__(self):
        #pass

    #def write_row(self, row):
        #"""
        #Row is a tuple with N + 1 elements matching the schema passed to get_writer.
        #The last element is a dict of columns not found in the schema
        #"""
        #raise Exception("unimplemented")

    #def close(self):
        #pass
