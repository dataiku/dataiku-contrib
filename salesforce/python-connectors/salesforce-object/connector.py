from dataiku.connector import Connector
import json
import salesforce


class MyConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)

        token = salesforce.get_json(self.config.get("token"))
        try:
            salesforce.API_BASE_URL = token.get('instance_url')
            salesforce.ACCESS_TOKEN = token.get('access_token')
        except Exception as e:
            raise ValueError("JSON token must contain access_token and instance_url")

        self.OBJECT = self.config.get("object", "")
        self.LIMIT = self.config.get("limit", "")
        self.RESULT_FORMAT = self.config.get("result_format")


    def get_read_schema(self):

        if self.RESULT_FORMAT == 'json':
            return {
                    "columns" : [
                        { "name" : "json", "type" : "object" }
                    ]
                }

        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        # First, building an SOQL query

        describe = salesforce.make_api_call('/services/data/v39.0/sobjects/%s/describe' % self.OBJECT)

        salesforce.log(describe)

        if not describe.get('queryable', False):
            raise ValueError("This object is not queryable")

        fields = [f['name'] for f in describe.get('fields')]

        query = "SELECT %s FROM %s LIMIT %i" % (
                ", ".join(fields),
                self.OBJECT,
                self.LIMIT
            )

        salesforce.log(query)

        # Then, running the SOQL query

        results = salesforce.make_api_call('/services/data/v39.0/queryAll/', {'q': query})

        #salesforce.log(results)

        salesforce.log("records_limit: %i" % records_limit)
        salesforce.log("length initial request: %i" % len(results.get('records')))

        n = 0

        for obj in results.get('records'):
            n = n + 1
            if records_limit < 0 or n <= records_limit:
                #salesforce.log("row %i" % n)
                yield self._format_row_for_dss(obj)

        next = results.get('nextRecordsUrl', None)
        if records_limit >= 0 and n >= records_limit:
            next = None

        while next:
            results = salesforce.make_api_call(next)
            for obj in results.get('records'):
                n = n + 1
                if records_limit < 0 or n <= records_limit:
                    #salesforce.log("row %i" % n)
                    yield self._format_row_for_dss(obj)
            next = results.get('nextRecordsUrl', None)
            if records_limit >= 0 and n >= records_limit:
                next = None


    def _format_row_for_dss(self, row):

        if self.RESULT_FORMAT == 'json':
            return {"json": json.dumps(row)}
        else:
            return salesforce.transform_json_to_dss_columns(row)
