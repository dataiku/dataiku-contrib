import requests, copy, json
from dataiku.connector import Connector
import dateutil.parser
import datetime


def maybe_json(dic, x):
    s = dic.get(x, None)
    if s is None:
        return ""
    else:
        return json.dumps(s)


class HipchatRoomHistoryConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)

        self.base_uri = "%s/v2/room/%s/history" %(self.config["api_endpoint"], self.config["room_name"])


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        beg_date_str = "%sT00:00:00Z" % partition_id
        beg_date = dateutil.parser.parse(beg_date_str)
        end_date = beg_date + datetime.timedelta(days=1)
        end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        first_data = self.get_first_page(beg_date_str, end_date_str)

        for item in first_data["items"]:
            yield self.to_row(item)

        next_uri = first_data["links"].get("next", None)

        while next_uri is not None:
            data = self.get_next_page(next_uri)
            next_uri = data["links"].get("next", None)
            for item in data["items"]:
                yield self.to_row(item)



    def get_read_schema(self):
        cnames = ["type", "author_name", "author_mention_name", "author_id", "notifier_name", "notifier_details",
                  "message", "date", "message_format", "color", "mentions", "id", "file", "message_links", "message_id"]

        columns = [{"name" : x, "type" : "date" if x == "date" else "string"} for x in cnames]

        return {"columns" : columns}

    def to_row(self, item):

        out = {}

        out["type"] = item["type"]

        if item["type"] == "message":
            author = item["from"]
            out["author_name"] = author["name"]
            out["author_mention_name"] = author["mention_name"]
            out["author_id"] = author["id"]
        else:
            out["notifier_name"] = item["from"]
            out["notifier_details"]= maybe_json(item, "notification_sender")

        for copyfield in ["message", "date", "message_format", "color"]:
            out[copyfield] = item.get(copyfield, "")

        out["mentions"] = maybe_json(item, "mentions")
        out["file"] = maybe_json(item, "file")
        out["message_links"] = maybe_json(item, "message_links")

        out["message_id"] = item.get("id")

        return out

    def get_first_page(self, beg_date, end_date):
        resp = requests.get(self.base_uri, params = {
            "end-date" : beg_date,
            "date" : end_date,
            "max-results" : 100,
            "auth_token" : self.config["auth_token"],
            "reverse" : False
        })
        resp.raise_for_status()
        j = resp.json()
        return j

    def get_next_page(self, uri):
        resp = requests.get(uri, params = {
            "auth_token" : self.config["auth_token"]
        })
        resp.raise_for_status()

        j = resp.json()
        return j

    def get_partitioning(self):
        return {
            "dimensions": [
                {
                    "name" : "day",
                    "type" : "time",
                    "params" : {
                        "period" : "DAY"
                    }

                }
            ]
        }