from dataiku.connector import Connector
from freshdesk_utils import FreshdeskConnector

class FreshdeskTicketsConnector(FreshdeskConnector, Connector):
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        FreshdeskConnector.__init__(self, config, plugin_config)
        view = config.get("view", '')
        if view == '':
            view = 'all_tickets'
        self.path = '/helpdesk/tickets/filter/' + view + '?format=json&wf_order=created_at&page='
