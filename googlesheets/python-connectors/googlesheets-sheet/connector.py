from dataiku.connector import Connector
import json
import gspread
from oauth2client.client import SignedJwtAssertionCredentials
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
        credentials = json.loads(self.config.get("credentials"))
        self.client_email = credentials["client_email"]
        self.private_key = credentials["private_key"]
        self.doc_id = self.config.get("doc_id")
        self.tab_id = self.config.get("tab_id")
        self.result_format = self.config.get("result_format")
        self.list_unique_slugs = []


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

        scope = ['https://spreadsheets.google.com/feeds']
        credentials = SignedJwtAssertionCredentials(self.client_email, self.private_key, scope)
        gc = gspread.authorize(credentials)

        ws = gc.open_by_key(self.doc_id).worksheet(self.tab_id)

        rows = ws.get_all_values()
        columns = rows[0]
        columns_slug = map(self.get_unique_slug, columns)

        if self.result_format == 'first-row-header':

            for row in rows[1:]:
                yield dict(zip(columns_slug,row)) 

        elif self.result_format == 'no-header':

            for row in rows:
                yield dict(zip(range(1, len(columns) + 1),row)) 

        elif self.result_format == 'array':

            for row in rows:
                yield {'array': json.dumps(row)} 

        else:

            raise Exception("Unimplemented")
