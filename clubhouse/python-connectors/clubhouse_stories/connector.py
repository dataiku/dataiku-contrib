from dataiku.connector import Connector
import datetime
import json
import logging
import unicodedata

import requests

def _byteify(data, ignore_dicts = False):
    # if this is a unicode string, return its string representation
    if isinstance(data, unicode):
        return unicodedata.normalize('NFKD', data).encode('ascii','ignore')
    # if this is a list of values, return list of byteified values
    if isinstance(data, list):
        return [ _byteify(item, ignore_dicts=True) for item in data ]
    # if this is a dictionary, return dictionary of byteified keys and values
    # but only if we haven't already byteified it
    if isinstance(data, dict) and not ignore_dicts:
        return {
            _byteify(key, ignore_dicts=True): _byteify(value, ignore_dicts=True)
            for key, value in data.iteritems()
        }
    # if it's anything else, return it in its original form
    return data                
    
class StoriesConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.endpoint = "https://api.clubhouse.io/api/beta/"
        self.key = "5bde183c-63d3-43e8-8be0-772ee2dc6cf3" #plugin_config["api_token"]

    def list_projects(self):
        logging.info("Clubhouse: fetching projects")
        return self.execute_query("projects")

    def list_stories(self, projectId):
        logging.info("Clubhouse: fetching stories for project %i" % projectId)
        return self.execute_query("projects/" + str(projectId) + "/stories")

    def execute_query(self, query):
        headers = {"Content-Type": "application/json"}

        r = requests.get(self.endpoint + query + "?token=" + self.key, headers=headers)
        r.raise_for_status()
        try:
            return _byteify(json.loads(r.content, object_hook=_byteify), ignore_dicts=True)
        except Exception:
            logging.info("Could not parse json from request content:\n" + r.content)
            raise

    def get_read_schema(self):
        # Let DSS infer the schema from the columns returned by the generate_rows method
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        query_date = datetime.datetime.now()

        nb = 0
        projects = self.list_projects()
        for project in projects:
            rows = self.list_stories(project['id'])
            for row in rows:
                if 0 <= records_limit <= nb:
                    logging.info("Reached records_limit (%i), stopping." % records_limit)
                    return

                row["query_date"] = query_date
                yield row
                nb += 1
