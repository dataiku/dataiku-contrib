import json, datetime, itertools, logging, json, pytz,requests, base64
from dataiku.connector import Connector

class FreshdeskTicketsConnector(Connector):
    def __init__(self, config):
        Connector.__init__(self, config)
        self.endpoint = config["endpoint"]
        self.key = config["apiKey"]

    def fetch_page(self, page):
        logging.info("Fetching tickets page %s" % page)
        base64string = base64.encodestring('%s:%s' % (self.key, "X"))
        auth = "Basic %s" % base64string
        headers = {'Authorization': auth}

        r = requests.get(self.endpoint + '/helpdesk/tickets/filter/all_tickets?format=json&wf_order=created_at&page='+str(page), headers = headers)
        return json.loads(r.content)

    def get_read_schema(self):
        # In Freshdesk, the schema depends on the columns selected by the user.
        # So we just return None and let the backend handle this for us
        return None

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        page = 1
        nb = 0
        query_date = datetime.datetime.now()

        while True:
            tickets = self.fetch_page(page)
            #logging.info("Fetched page %s" % tickets)

            if len(tickets) == 0:
                return
            else:
                for ticket in tickets:
                    if records_limit >= 0 and nb >= records_limit:
                        return

                    ticket["query_date"] = query_date

                    # "Degroup" the custom fields
                    if "custom_field" in ticket:
                        for (k, v) in ticket["custom_field"].items():
                            ticket["custom_field_%s" %k ] = v
                        del ticket["custom_field"]

                    logging.info("Freshdesk connector yields ticket")
                    yield ticket
                    nb +=1
            page += 1