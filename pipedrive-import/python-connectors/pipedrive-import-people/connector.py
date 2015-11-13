# This file is the actual code for the custom Python dataset people

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

        persons = make_api_call_all_pages(self.CONFIG_API, 'persons') #TODO: should not get all persons when not needed (previews, small sample...)
        fields = make_api_call_all_pages(self.CONFIG_API, 'personFields')

        for field in fields:
            field['name_slugified'] = get_unique_slug(field['name'])

        for person in persons:
            if self.RESULT_FORMAT == 'json':
                row = {"json": json.dumps(person)}
            else:
                row = {}
                for field in fields:
                    column_key = field['key']
                    column_name = field['name_slugified']
                    column_type = field['field_type']
                    value = person[column_key] if column_key in person else None;
                    #print "%s -> %s (%s) : %s" % (column_key, column_name, column_type, value)

                    if value is None:
                        row[column_name] = ''
                    elif column_key in ['email', 'phone', 'im']:
                        row[column_name] = value[0]['value']
                    elif column_type in ['varchar', 'varchar_auto', 'double', 'int', 'text', 'date', 'time', 'monetary', 'stage', 'phone']:
                        row[column_name] = value
                    elif column_type in ['org', 'people', 'user', 'json']:
                        row[column_name] = value['name'] if isinstance(value,dict) else value
                        #note: inline if because sometimes person_id is not a dict... bug in the API?
                    elif column_type in ['enum', 'visible_to', 'status', 'set']:
                        for o in field['options']: #TODO: this could be out of the person+field loop
                            if str(o['id']) == str(value):
                                row[column_name] = o['label']
                    elif column_type == 'picture':
                        if isinstance(value,dict):
                            if '512' in value['pictures']:
                                row[column_name] = value['pictures']['512']
                            elif '128' in value['pictures']:
                                row[column_name] = value['pictures']['128']
                            else:
                                row[column_name] = value['pictures']
                        else:
                            row[column_name] = value
                    else:
                        print "Pipedrive unknown type %s" % column_type
                        row[column_name] = value
            yield row
        
