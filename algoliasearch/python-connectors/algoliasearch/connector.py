import json, datetime, itertools
from algoliasearch import algoliasearch
from dataiku.connector import Connector, CustomDatasetWriter
import logging

class AlgoliaSearchConnector(Connector):
    def __init__(self, config):
        Connector.__init__(self, config)

    def get_read_schema(self):
        # The Algolia connector does not provide a schema, since
        # the index is schemaless
        return None

    def _get_index(self):
        client = algoliasearch.Client(self.config["applicationId"], self.config["apiKey"])
        index = client.init_index(self.config["index"])
        return index

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):
        index = self._get_index()

        search_settings = {
            "attributesToHighlight" : []
        }
        if records_limit >= 0:
            search_settings["hitsPerPage"] = records_limit

        res = index.search(self.config.get("searchQuery", ""), search_settings)

        for hit in res["hits"]:
            if "_highlightResult" in hit:
                del hit["_highlightResult"]

            yield hit

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):

        return AlgoliaSearchConnectorWriter(self.config, self._get_index(),
                    dataset_schema, dataset_partitioning, partition_id)

class AlgoliaSearchConnectorWriter(CustomDatasetWriter):
    def __init__(self, config, index, dataset_schema, dataset_partitioning, partition_id):
        CustomDatasetWriter.__init__(self)
        self.config = config
        self.index = index
        self.dataset_schema = dataset_schema
        self.dataset_partitioning = dataset_partitioning
        self.partition_id = partition_id

        self.buffer = []
        self.index.clear_index()
        # If we are partitioned, clear by query
        # TODO

    def write_row(self, row):
        logging.info("Algolia Write")
        obj = {}
        for (col, val) in zip(self.dataset_schema["columns"], row):
            #logging.info("Write %s for %s" % (val, col))
            if len(unicode(val)) > 5000:
                val = unicode(val)[0:5000]
            obj[col["name"]] = val
            if col["name"] =="id":
                logging.info("Set ObjectID")
                obj["objectID"] = val
        self.buffer.append(obj)

        if len(self.buffer) > 50:
            self.flush()

    def flush(self):
        logging.info("Flushing Algolia buffer")
        self.index.save_objects(self.buffer)
        self.buffer = []

    def close(self):
        self.flush()
        logging.info("Closing Algolia")
