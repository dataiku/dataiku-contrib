from dataiku.connector import Connector
import datetime
import json
import logging
from utils import byteify

import requests


class QueryConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.shortcut_app_url_template = "https://api.app.shortcut.com{api_suffix}"
        self.key = plugin_config["api_token"]
        self.search_query = config.get("search_query", "")

    def execute_search_query(self, search_query_url, records_limit, current_number_of_result=0):
        endpoint_with_token_template = "{endpoint}&token={token}"
        headers = {"Content-Type": "application/json"}
        logging.info("Fetching data from: {}".format(search_query_url))
        r = requests.get(endpoint_with_token_template.format(endpoint=search_query_url, token=self.key), headers=headers)
        r.raise_for_status()
        try:
            query_result = byteify(json.loads(r.content, object_hook=byteify), ignore_dicts=True)
            result = query_result["data"]
            new_number_of_result = current_number_of_result + len(result)
            if 0 <= records_limit <= new_number_of_result:
                logging.info("Reached records_limit ({}), stopping recursive queries to get next page. "
                             "Current number of results: {}.".format(records_limit, new_number_of_result))
                return result
            next_page_url_suffix = query_result["next"]
            if next_page_url_suffix is not None:
                result = result + self.execute_search_query(
                    self.shortcut_app_url_template.format(api_suffix=next_page_url_suffix),
                    records_limit,
                    new_number_of_result
                )
            return result
        except Exception:
            logging.info("Could not parse json from request content:\n" + r.content)
            raise

    def execute_search_queries(self, records_limit):
        # Can retrieve max 25 items for each page
        search_query_url = self.shortcut_app_url_template.format(
            api_suffix="/api/v3/search/stories?query={search_query}&page_size=25".format(search_query=self.search_query))
        return self.execute_search_query(search_query_url, records_limit)

    def get_read_schema(self):
        # Let DSS infer the schema from the columns returned by the generate_rows method
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                      partition_id=None, records_limit=-1):
        query_date = datetime.datetime.now()

        nb = 0
        for row in self.execute_search_queries(records_limit):
            if 0 <= records_limit <= nb:
                logging.info("Reached records_limit ({}), stopping.".format(records_limit))
                return

            row["query_date"] = query_date
            yield row
            nb += 1
