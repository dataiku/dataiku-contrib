from dataiku.connector import Connector
from airtable import airtable_api

class MyConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class

        self.base = self.config.get("base")
        self.table = self.config.get("table")
        self.key = self.config.get("key")

        if self.base is None or self.table is None or self.key is None:
            raise ValueError("Missing parameters (Base ID, or Table name, or API key")

    def get_read_schema(self):
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):


        looping = True
        offset = None
        params = {
            "pageSize": 100
        }

        # if sort_by is not None:
        #     params.update({
        #         "sort[0][field]": sort_by,
        #         "sort[0][direction]": sort_order}
        #     )

        while looping:
            if offset != None:
                params.update({'offset':offset})
            results = airtable_api(self.base, self.table, self.key, parameters=params)
            for record in results.get("records"):
                yield record["fields"]
            offset = results.get("offset")
            looping = False if offset is None else True


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

