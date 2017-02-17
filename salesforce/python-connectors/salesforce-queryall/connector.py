from dataiku.connector import Connector
import json
import salesforce


class MyConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)

        token = self.config.get("token")
        try:
            token = json.loads(token)
            salesforce.API_BASE_URL = token.get('instance_url')
            salesforce.ACCESS_TOKEN = token.get('access_token')
        except Exception as e:
            raise ValueError("Unvalid JSON token")

        self.QUERY = self.config.get("query", "")
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

        results = salesforce.make_api_call('/services/data/v39.0/queryAll/', {'q': self.QUERY})

        salesforce.log(results)

        for obj in results.get('records'):
            yield self._format_row_for_dss(obj)

        next = results.get('nextRecordsUrl', None)

        while next:
            results = salesforce.make_api_call(next)
            for obj in results.get('records'):
                yield self._format_row_for_dss(obj)
            next = results.get('nextRecordsUrl', None)


    def _format_row_for_dss(self, row):

        if self.RESULT_FORMAT == 'json':
            return {"json": json.dumps(row)}
        else:
            return salesforce.transform_json_to_dss_columns(row)
