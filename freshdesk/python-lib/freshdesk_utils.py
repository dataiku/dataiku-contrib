import json, datetime, logging, json, requests, base64

class FreshdeskConnector():
    def __init__(self, config):
        self.endpoint = config["endpoint"]
        self.key = config["apiKey"]

    def fetch_page(self, page):
        logging.info("Freshdesk: fetching page %s" % page)
        base64string = base64.encodestring('%s:%s' % (self.key, "X")).replace('\n','')
        auth = "Basic %s" % base64string
        headers = {'Authorization': auth}

        r = requests.get(self.endpoint + self.path+str(page), headers = headers)
        r.raise_for_status()
        try:
            return json.loads(r.content)
        except Exception:
            logging.info("Could not parse json from request content:\n" + r.content)
            raise

    def get_read_schema(self):
        # In Freshdesk, the schema depends on the custom fields defined by the user.
        # So we just return None and let the backend handle this for us
        return None

    def extract_json_subelement(self,row):
        return row

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        page = 1
        nb = 0
        query_date = datetime.datetime.now()

        while True:
            rows = self.fetch_page(page)
            if len(rows) == 0:
                logging.info("Page %i is empty, stopping." % page)
                return
            else:
                for row in rows:
                    # row can be a ticket or a user (depending on the class subclassing FreshdeskConnector)
                    row = self.extract_json_subelement(row)
                    if records_limit >= 0 and nb >= records_limit:
                        logging.info("Reached records_limit (%i), stopping." % records_limit)
                        return

                    row["query_date"] = query_date

                    # Flatten the custom fields
                    if "custom_field" in row:
                        for (k, v) in row["custom_field"].items():
                            row["custom_field_%s" %k ] = v
                        del row["custom_field"]

                    yield row
                    nb +=1
            page += 1
