from dataiku.connector import Connector
from freshdesk_utils import FreshdeskConnector

class FreshDeskUsersConnector(FreshdeskConnector, Connector):
    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)
        FreshdeskConnector.__init__(self, config, plugin_config)
        self.path = '/contacts.json?page='

    def extract_json_subelement(self,user):
        return user['user']
