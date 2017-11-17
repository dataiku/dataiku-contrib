# -*- coding: utf-8 -*-

from dataiku.connector import Connector
from datarendezvous_api import DrdvApi

class DrdvConnector(Connector):

    def __init__(self, config, plugin_config):

        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        # Form data
        self.api_key = self.config.get("api_key", None)
        self.dataset_id = self.config.get("dataset_id", None)
        
        self.api = DrdvApi(self.api_key)

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        
        records = self.api.export(self.dataset_id, records_limit)     
        for row in records:
            del row['_id']
            yield row


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        raise Exception("Unimplemented")


    def get_partitioning(self):
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        return []


    def partition_exists(self, partitioning, partition_id):
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        raise Exception("unimplemented")


class CustomDatasetWriter(object):
    def __init__(self):
        pass

    def write_row(self, row):
        raise Exception("unimplemented")

    def close(self):
        pass
