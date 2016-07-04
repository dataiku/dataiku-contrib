from dataiku.connector import Connector, CustomDatasetWriter
import json
import gspread
import oauth2client
from oauth2client.service_account import ServiceAccountCredentials
from slugify import slugify

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
        self.credentials = json.loads(self.config.get("credentials"))
        self.doc_id = self.config.get("doc_id")
        self.tab_id = self.config.get("tab_id")
        self.result_format = self.config.get("result_format")
        self.list_unique_slugs = []


    def get_spreadsheet(self):

        scope = ['https://spreadsheets.google.com/feeds']
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials, scope)
        gc = gspread.authorize(credentials)

        return gc.open_by_key(self.doc_id).worksheet(self.tab_id)

    def get_unique_slug(self, string):
        string = slugify(string, to_lower=False,max_length=25,separator="_",capitalize=False)
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
        ws = self.get_spreadsheet()
        rows = ws.get_all_values()
        columns = rows[0]
        columns_slug = map(self.get_unique_slug, columns)

        if self.result_format == 'first-row-header':

            for row in rows[1:]:
                yield dict(zip(columns_slug,row)) 

        elif self.result_format == 'no-header':

            for row in rows:
                yield dict(zip(range(1, len(columns) + 1),row))

        else:

            raise Exception("Unimplemented")


    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                         partition_id=None):

        return MyCustomDatasetWriter(self.config, self, dataset_schema, dataset_partitioning, partition_id)


    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        ws = self.get_spreadsheet()

        if self.result_format == 'first-row-header':

            return ws.row_count - 1

        elif self.result_format == 'no-header':

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

        self.LIMIT_COLUMNS = 256
        self.LIMIT_CELLS = 400000
        self.LIMIT_LINES = 2000 #this is not an official limit

        columns = [col["name"] for col in dataset_schema["columns"]]

        if len(columns) > self.LIMIT_COLUMNS:
            raise Exception("A spreadsheet cannot contain more than %i columns." % self.LIMIT_COLUMNS)

        # Example of dataset_schema: {u'userModified': False, u'columns': [{u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'condition', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'weather', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'double', u'name': u'temperature', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'bigint', u'name': u'humidity', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_update', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_add', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'ville', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'source', u'maxLength': -1}]}

        # TODO:
        # - Verify the size of the spreadsheet
        # - Implement the flush
        # - Clean outside of what is written
        # - Implement the limit on number of rows

        if parent.result_format == 'first-row-header':

            self.buffer.append(columns)


    def write_row(self, row):

        # for (col, val) in zip(self.dataset_schema["columns"], row):
        #     print (col, val)

        self.buffer.append(row)

        # if len(self.buffer) > 50:
        #     self.flush()


    def flush(self):
        ws = self.parent.get_spreadsheet()

        num_columns = len(self.buffer[0])
        num_lines = len(self.buffer)

        if num_lines > self.LIMIT_LINES:
            raise Exception("A spreadsheet cannot contain more than %i lines." % self.LIMIT_LINES)

        if num_lines * num_columns > self.LIMIT_CELLS:
            raise Exception("A spreadsheet cannot contain more than %i cells." % self.LIMIT_CELLS)

        ws.resize(rows=num_lines, cols=num_columns)

        cell_list = ws.range( 'A1:%s' % ws.get_addr_int(num_lines, num_columns) )
        for cell in cell_list:
            val = self.buffer[cell.row-1][cell.col-1]
            # if type(val) is str:
            #     val = val.decode('utf-8')
            cell.value = val
        ws.update_cells(cell_list)

        self.buffer = []

    def close(self):
        self.flush()
        pass
        

