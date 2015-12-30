from dataiku.connector import Connector
from freshdesk_utils import FreshdeskConnector

class FreshdeskTicketsCustomViewConnector(FreshdeskConnector, Connector):
    def __init__(self, config):
        Connector.__init__(self, config)
        FreshdeskConnector.__init__(self, config)
        self.view = config["view"]
        self.path = '/helpdesk/tickets/filter/' + str(self.view) + '?format=json&wf_order=created_at&page='
        print self.path	