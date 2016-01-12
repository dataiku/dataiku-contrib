# This file is the actual code for the custom Python dataset deals

from dataiku.connector import Connector
import json
from pipedriveapi import *


class MyConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)

        self.CONFIG_API = {
            'API_BASE_URL': 'https://api.pipedrive.com/v1/',
            'API_KEY': self.config.get("api_key"),
            'PAGINATION': 200
        }
        self.RESULT_FORMAT = self.config.get("result_format")



    def get_read_schema(self):

        if self.RESULT_FORMAT == 'json':
            return {
                    "columns" : [
                        { "name" : "json", "type" : "json" }
                    ]
                }

        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        deals = make_api_call_all_pages(self.CONFIG_API, 'deals') #TODO: should not get all deals when not needed (previews, small sample...)
        fields = make_api_call_all_pages(self.CONFIG_API, 'dealFields')

        for field in fields:
            field['name_slugified'] = get_unique_slug(field['name'])

        for deal in deals:
            if self.RESULT_FORMAT == 'json':
                row = {"json": json.dumps(deal)}
            else:
                row = {}
                for field in fields:
                    column_key = field['key']
                    column_name = field['name_slugified']
                    column_type = field['field_type']
                    value = deal[column_key] if column_key in deal else None;
                    #print "%s -> %s (%s) : %s" % (column_key, column_name, column_type, value)

                    if column_key == 'pipeline':
                        row[column_name] = deal['pipeline_id']
                    elif value is None:
                        row[column_name] = ''
                    elif column_type in ['varchar', 'varchar_auto', 'double', 'int', 'text', 'time', 'monetary', 'stage', 'phone']:
                        row[column_name] = value
                    elif column_type in ['date', 'daterange']:
                        row[column_name] = parse_date(value)
                    elif column_type in ['org', 'people', 'user']:
                        row[column_name] = value['name'] if isinstance(value,dict) else value
                        #note: inline if because sometimes person_id is not a dict... bug in the API?
                    elif column_type in ['enum', 'visible_to', 'status', 'set']:
                        for o in field['options']: #TODO: this could be out of the deal+field loop
                            if str(o['id']) == str(value):
                                row[column_name] = o['label']
                    else:
                        print "Pipedrive unknown type %s" % column_type
                        row[column_name] = value
            yield row
        
