from dataiku.connector import Connector, CustomDatasetWriter
import json
import os.path
from collections import OrderedDict
import gspread
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials
from slugify import slugify

class MyConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)  # pass the parameters to the base class
        self.credentials = self.config.get("credentials")
        self.doc_id = self.config.get("doc_id")
        self.tab_id = self.config.get("tab_id")
        self.result_format = self.config.get("result_format")
        self.list_unique_slugs = []

        file = self.credentials.splitlines()[0]
        if os.path.isfile(file):
            try:
                with open(file, 'r') as f:
                    self.credentials  = json.load(f)
                    f.close()
            except Exception as e:
                raise ValueError("Unable to read the JSON Service Account from file '%s'.\n%s" % (file, e))
        else:
            try:
                self.credentials  = json.loads(self.credentials)
            except Exception as e:
                raise Exception("Unable to read the JSON Service Account.\n%s" % e)

        scope = [
            'https://www.googleapis.com/auth/spreadsheets',
            #'https://www.googleapis.com/auth/drive'
        ]
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(self.credentials, scope)
        self.gspread_client = gspread.authorize(credentials)


    def get_spreadsheet(self):

        try:
            return self.gspread_client.open_by_key(self.doc_id).worksheet(self.tab_id)
        except gspread.exceptions.SpreadsheetNotFound as e:
            raise Exception("Trying to open non-existent or inaccessible spreadsheet document.")
        except gspread.exceptions.WorksheetNotFound as e:
            raise Exception("Trying to open non-existent sheet. Verify that the sheet name exists (%s)." % self.tab_id)
        except gspread.exceptions.APIError as e:
            if hasattr(e, 'response'):
                error_json = e.response.json()
                print(error_json)
                error_status = error_json.get("error", {}).get("status")
                email = self.credentials.get("client_email", "(email missing)")
                if error_status == 'PERMISSION_DENIED':
                    raise Exception("The Service Account does not have permission to read or write on the spreadsheet document. Have you shared the spreadsheet with %s?" % email)
                if error_status == 'NOT_FOUND':
                    raise Exception("Trying to open non-existent spreadsheet document. Verify the document id exists (%s)." % self.doc_id)
            raise Exception("The Google API returned an error: %s" % e)


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
        ws = self.get_spreadsheet()
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

        # TODO:
        # - Implement the flush
        # - Handle types?


    def write_row(self, row):

        # Example of dataset_schema: {u'userModified': False, u'columns': [{u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'condition', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'weather', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'double', u'name': u'temperature', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'bigint', u'name': u'humidity', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_update', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'date', u'name': u'date_add', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'ville', u'maxLength': -1}, {u'timestampNoTzAsDate': False, u'type': u'string', u'name': u'source', u'maxLength': -1}]}
        # for (col, val) in zip(self.dataset_schema["columns"], row):
        #     print (col, val)

        self.buffer.append(row)

        # if len(self.buffer) > 50:
        #     self.flush()


    def flush(self):
        ws = self.parent.get_spreadsheet()

        num_columns = len(self.buffer[0])
        num_lines = len(self.buffer)

        ws.resize(rows=num_lines, cols=num_columns)

        cell_list = ws.range( 'A1:%s' % rowcol_to_a1(num_lines, num_columns) )
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
        

