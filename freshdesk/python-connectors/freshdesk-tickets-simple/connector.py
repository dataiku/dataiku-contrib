from dataiku.connector import Connector
from freshdesk_utils import FreshdeskConnector

class FreshdeskTicketsConnector(FreshdeskConnector, Connector):
    def __init__(self, config):
        Connector.__init__(self, config)
        FreshdeskConnector.__init__(self, config)
        self.path = '/helpdesk/tickets/filter/all_tickets?format=json&wf_order=created_at&page='