from dataiku.connector import Connector
import datetime
import json
import logging

import requests

class MilestonesConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.endpoint = "https://api.clubhouse.io/api/beta"
        self.key = "5bde183c-63d3-43e8-8be0-772ee2dc6cf3" #plugin_config["api_token"]

    def list_milestones(self):
        logging.info("Clubhouse: fetching epics")
        headers = {"Content-Type": "application/json"}

        r = requests.get(self.endpoint + "/milestones?token=" + self.key, headers=headers)
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

        rows = self.list_milestones()
        nb = 0
        for row in rows:
            if 0 <= records_limit <= nb:
                logging.info("Reached records_limit (%i), stopping." % records_limit)
                return

            row[u"query_date"] = query_date
            yield row
            nb += 1
