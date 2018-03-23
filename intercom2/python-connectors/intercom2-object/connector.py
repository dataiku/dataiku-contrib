from dataiku.connector import Connector
import json
from intercomapi import *


class MyConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)

        self.TOKEN = self.config.get("token", "")
        self.OBJECT = self.config.get("object", "")
        self.RESULT_FORMAT = self.config.get("result_format", "readable")


    def get_read_schema(self):

        if self.RESULT_FORMAT == 'json':
            return {
                    "columns" : [
                        { "name" : "json", "type" : "json" }
                    ]
                }

        return None


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None, partition_id=None, records_limit=-1):

        # mapping each object to the corresponding function
        object_to_func = {
            "users": scroll_object,
            "contacts": scroll_object,
            "companies": scroll_object,
            "admins": list_object,
            "conversations": list_object,
            "tags": list_object,
            "segments": list_object
        }

        if self.OBJECT not in object_to_func.keys():
            raise Exception("Unknown object")

        log("records_limit: %i" % records_limit)
        n = 0

        for item in object_to_func[self.OBJECT](self.OBJECT, self.TOKEN):
            n = n + 1
            if self.RESULT_FORMAT == 'json':
                yield {"json": json.dumps(item)}
            else:
                yield make_row_compatible_with_dss_schema(item)

            # We try to respect records_limit
            # but when using the scroll endpoint, it is better to paginate until the end not to keep an open scroll.
            # From Itercom documentation :
            # "Each app can only have 1 scroll open at a time. You'll get an error message if you try to have more than one open per app.
            # If the scroll isn't used for 1 minute, it expires and calls with that scroll param will fail
            # If the end of the scroll is reached, the scroll parameter will expire"
            if n >= records_limit and object_to_func[self.OBJECT] != scroll_object:
                break

