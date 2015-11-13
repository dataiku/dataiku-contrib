# This file is the actual code for the custom Python dataset deals

# import the base class for the custom dataset
from dataiku.connector import Connector
import json
from pipedriveapi import *


"""
A custom Python dataset is a subclass of Connector.

The parameters it expects and some flags to control its handling by DSS are
specified in the connector.json file.

Note: the name of the class itself is not relevant
"""
class MyConnector(Connector):

    def __init__(self, config):
        """
        The configuration parameters set up by the user in the settings tab of the
        dataset are passed as a json object 'config' to the constructor
        """
        Connector.__init__(self, config)  # pass the parameters to the base class

        # perform some more initialization
        self.CONFIG_API = {
            'API_BASE_URL': 'https://api.pipedrive.com/v1/',
            'API_KEY': self.config.get("api_key"),
            'PAGINATION': 200
        }
        self.RESULT_FORMAT = self.config.get("result_format")



    def get_read_schema(self):
        """
        Returns the schema that this connector generates when returning rows.

        The returned schema may be None if the schema is not known in advance.
        In that case, the dataset schema will be infered from the first rows.

        Whether additional columns returned by the generate_rows are kept is configured
        in the connector.json with the "strictSchema" field
        """

        if self.RESULT_FORMAT == 'json':
            return {
                    "columns" : [
                        { "name" : "json", "type" : "json" }
                    ]
                }

        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """

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
                    elif column_type in ['varchar', 'varchar_auto', 'double', 'int', 'text', 'date', 'time', 'monetary', 'stage', 'phone']:
                        row[column_name] = value
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
        


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):
        """
        Returns a write object to write in the dataset (or in a partition)

        The dataset_schema given here will match the the rows passed in to the writer.

        Note: the writer is responsible for clearing the partition, if relevant
        """
        raise Exception("Unimplemented")


    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise Exception("Unimplemented")

    def get_records_count(self, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the field "canCountRecords" is set to
        true in the connector.json
        """
        raise Exception("unimplemented")


class CustomDatasetWriter(object):
    def __init__(self):
        pass

    def write_row(self, row):
        """
        Row is a tuple with N + 1 elements matching the schema passed to get_writer.
        The last element is a dict of columns not found in the schema
        """
        raise Exception("unimplemented")

    def close(self):
        pass