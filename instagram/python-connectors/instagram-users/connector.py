import json
import requests
import datetime
from dataiku.connector import Connector

class InstagramConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        print "=============================================================="
        print self.config
        print self.plugin_config
        self.ACCOUNT_ID   = self.config.get("account_id", None)
        self.ACCESS_TOKEN = self.config.get("access_token", None)
        self.ACCOUNT_LIST = self.config.get("account_list", None)        
        self.session = requests.Session()
        self.API = 'https://api.instagram.com/v1'
        self.IS_LIST = self.ACCOUNT_LIST is not None
        print self.IS_LIST

    def get_read_schema(self):
        schema = {
            "columns": [
                {'name':'record', 'type':'string'}
            ]
        }
        return schema

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        if not self.IS_LIST:
            d = {}
            d['dku_params'] = {}
            d['dku_data'] = None
            d['dku_errors'] = None
            # Input
            d['dku_params'] = {}
            d['dku_params']['fetched_at'] = datetime.datetime.utcnow().isoformat()
            d['dku_params']['account_id'] = self.ACCOUNT_ID
            d['dku_params']['access_token'] = self.ACCESS_TOKEN
            # Output / API specific
            params = {}
            params['access_token'] = self.ACCESS_TOKEN
            url = self.API + '/users/{}'.format(self.ACCOUNT_ID)
            r = self.session.get(url, params=params)
            try:
                d['dku_data'] = r.json()
            except Exception, e:
                d['dku_error'] = str(e)
            yield {'record': json.dumps(d)}
        else:
            accounts = self.ACCOUNT_LIST.split('\n')
            for account in accounts:
                account_id, access_token = account.strip().split('\t')
                d = {}
                d['dku_params'] = {}
                d['dku_data'] = None
                d['dku_errors'] = None
                # Input
                d['dku_params'] = {}
                d['dku_params']['fetched_at'] = datetime.datetime.utcnow().isoformat()
                d['dku_params']['account_id'] = account_id
                d['dku_params']['access_token'] = access_token
                # Output / API specific
                params = {}
                params['access_token'] = access_token
                url = self.API + '/users/{}'.format(account_id)
                r = self.session.get(url, params=params)
                try:
                    d['dku_data'] = r.json()
                except Exception, e:
                    d['dku_error'] = str(e)
                yield {'record': json.dumps(d)}
                


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        raise Exception("Unimplemented")


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise Exception("Unimplemented")


    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []


    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise Exception("unimplemented")
