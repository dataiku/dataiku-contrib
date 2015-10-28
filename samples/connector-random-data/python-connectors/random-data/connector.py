from dataiku.connector import Connector

class RandomDataConnector(Connector):
    def __init__(self, config):
        Connector.__init__(self, config)

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):
        for i in xrange(1, 10):
            yield {
                "column1" : "val1_%s" % i,
                "column2" : "val2",
                "column3" : 3
            }

    def get_read_schema(self):
        return {
            "columns" : [
                {"name":  "column1", "type" : "string"},
                {"name":  "column2", "type" : "string"},
                {"name":  "column3", "type" : "int"}
            ]
        }