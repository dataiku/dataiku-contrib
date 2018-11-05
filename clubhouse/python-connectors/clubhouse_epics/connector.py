from dataiku.connector import Connector
import datetime
import json
import logging

import requests

class EpicsConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.endpoint = "https://api.clubhouse.io/api/beta"
        self.key = plugin_config["api_token"]

    def list_epics(self):
        logging.info("Clubhouse: fetching epics")
        headers = {"Content-Type": "application/json"}

        r = requests.get(self.endpoint + "/epics?token=" + self.key, headers=headers)
        r.raise_for_status()
        try:
            return json.loads(r.content)
        except Exception:
            logging.info("Could not parse json from request content:\n" + r.content)
            raise

    def get_read_schema(self):
        # Let DSS infer the schema from the columns returned by the generate_rows method
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        query_date = datetime.datetime.now()

        rows = self.list_epics()
        if len(rows) == 0:
            logging.info("Not epics.")
        else:
            nb = 0
            for row in rows:
                if 0 <= records_limit <= nb:
                    logging.info("Reached records_limit (%i), stopping." % records_limit)
                    return

                res = {}
                row[u"query_date"] = query_date
                yield row
                nb += 1


    def get_writer(self, dataset_schema=None, dataset_partitioning=None, partition_id=None):
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
