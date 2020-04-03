from dataiku.connector import Connector, CustomDatasetWriter
import json
from collections import OrderedDict
from gspread.utils import rowcol_to_a1
from slugify import slugify
from googlesheets import get_spreadsheet

class MyConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)  # pass the parameters to the base class
        self.credentials = self.config.get("credentials")
        self.doc_id = self.config.get("doc_id")
        self.tab_id = self.config.get("tab_id")
        self.result_format = self.config.get("result_format")
        self.write_format = self.config.get("write_format")
        self.list_unique_slugs = []


    def get_unique_slug(self, string):
        string = slugify(string, max_length=25, separator="_", lowercase=False)
        if string == '':
            string = 'none'
        test_string = string
        i = 0
        while test_string in self.list_unique_slugs:
            i += 1
            test_string = string + '_' + str(i)
        self.list_unique_slugs.append(test_string)
        return test_string


    def get_read_schema(self):
        # The Google Spreadsheets connector does not have a fixed schema, since each
        # sheet has its own (varying) schema.
        #
        # Better let DSS handle this
        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        """
        The main reading method.

        Returns a generator over the rows of the dataset (or partition)
        Each yielded row must be a dictionary, indexed by column name.

        The dataset schema and partitioning are given for information purpose.
        """
        ws = get_spreadsheet(self.credentials, self.doc_id, self.tab_id)
        rows = ws.get_all_values()
        columns = rows[0]
        columns_slug = list(map(self.get_unique_slug, columns))

        if self.result_format == 'first-row-header':

            for row in rows[1:]:
                yield OrderedDict(zip(columns_slug, row))

        elif self.result_format == 'no-header':

            for row in rows:
                yield OrderedDict(zip(range(1, len(columns) + 1), row))

        elif self.result_format == 'json':

            for row in rows:
                yield {"json": json.dumps(row)}

        else:

            raise Exception("Unimplemented")


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):

        if self.result_format == 'json':

            raise Exception('JSON format not supported in write mode')

        return MyCustomDatasetWriter(self.config, self, dataset_schema, dataset_partitioning, partition_id)


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).
        """
        ws = self.get_spreadsheet()

        if self.result_format == 'first-row-header':

            return ws.row_count - 1

        elif self.result_format in ['no-header', 'json']:

            return ws.row_count

        else:

            return 0




class MyCustomDatasetWriter(CustomDatasetWriter):
    def __init__(self, config, parent, dataset_schema, dataset_partitioning, partition_id):
        CustomDatasetWriter.__init__(self)
        self.parent = parent
        self.config = config
        self.dataset_schema = dataset_schema
        self.dataset_partitioning = dataset_partitioning
        self.partition_id = partition_id

        self.buffer = []

        columns = [col["name"] for col in dataset_schema["columns"]]

        if parent.result_format == 'first-row-header':
            self.buffer.append(columns)


    def write_row(self, row):

        # Example of dataset_schema: {u'userModified': False, u'columns': [{u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'condition', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'weather', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'double', u'name': u'temperature', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'bigint', u'name': u'humidity', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_update', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_add', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'ville', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'source', u'maxLength': -1}]}
        # for (col, val) in zip(self.dataset_schema["columns"], row):
        #     print (col, val)

        self.buffer.append(row)

        # if len(self.buffer) > 50:
        #     self.flush()


    def flush(self):
        ws = get_spreadsheet(self.parent.credentials, self.parent.doc_id, self.parent.tab_id)

        num_columns = len(self.buffer[0])
        num_lines = len(self.buffer)

        ws.resize(rows=num_lines, cols=num_columns)

        range = 'A1:%s' % rowcol_to_a1(num_lines, num_columns)
        ws.update(range, self.buffer, value_input_option=self.parent.write_format)

        self.buffer = []

    def close(self):
        self.flush()
        pass
        

