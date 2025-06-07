import requests
import json
import pandas as pd
from dataiku.connector import Connector
import importio_utils
import logging

logger = logging.getLogger(__name__)


class ImportIOConnector(Connector):

    def __init__(self, config):
        """Make the only API call, which downloads the data"""
        Connector.__init__(self, config)
        if self.config['api_url'].startswith('https://api.import.io/'):
            self.api_version = 'api'
        elif self.config['api_url'].startswith('https://extraction.import.io/'):
            self.api_version = 'extraction'
        else:
            raise Exception('It looks like this URL is not an API URL. URLs to call the API (and get a json response) start with "https://api.import.io" .')
        logger.info('[import.io connector] calling API...')
        response = requests.get(self.config['api_url'])
        logger.info('[import.io connector] got response')
        try:
            self.json = response.json()
        except Exception as e:
            logger.error(e)
            logger.error('response was:{}'.format(response.text))
            raise ValueError

    def get_read_schema(self):
        if self.api_version == 'api':
            columns = importio_utils.convert_schema(self.json['outputProperties'])
            return {"columns": columns}
        else:
            return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
        if self.api_version == 'api':
            for row in self.json['results']:
                yield row
        else:
            df = pd.DataFrame(self.json['extractorData']['data'][0]['group'])
            for col in df.columns:
                lengths = df[col].apply(lambda x: len(x) if type(x) == list else 0)
                if lengths.max() == 1:
                    df[col] = df[col].apply(lambda x: x[0] if type(x) == list else {})
                    keys = df[col].apply(lambda x: x.keys())
                    for key in set([key for line in keys for key in line]): # drop duplicates
                        df[col + '_' + key] = df[col].apply(lambda x: x.get(key,''))
                    del df[col]
                else:
                    df[col] = df[col].apply(json.dumps)
            for row in df.to_dict(orient='records'):
                yield row
