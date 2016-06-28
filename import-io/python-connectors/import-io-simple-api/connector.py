import requests
from dataiku.connector import Connector
import importio_utils

class ImportIOConnector(Connector):

    def __init__(self, config):
        """Make the only API call, which downloads the data"""
        Connector.__init__(self, config)
        if not self.config['api_url'].startswith('https://api.import.io/'):
            raise Exception(
                'It looks like this URL is not an API URL. URLs to call the API (and get a json response) start with "https://api.import.io" .')
        print '[import.io connector] calling API...'
        response = requests.get(self.config['api_url'])
        print '[import.io connector] got response'
        try:
            self.json = response.json()
        except Exception as e:
            print e
            print 'response was:\n', response.text
            raise

    def get_read_schema(self):
        columns = importio_utils.convert_schema(self.json['outputProperties'])
        return {"columns":columns}

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit = -1):
        for row in self.json['results']:
            yield row
