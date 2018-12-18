import os, json
import logging
from dataiku.cluster import Cluster
import dku_hdi
from azure.common.credentials import InteractiveCredentials, ServicePrincipalCredentials, UserPassCredentials

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class MyCluster(Cluster):
    def __init__(self, cluster_id, cluster_name, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config
        
        #TODO: Deal with different auth types
        self.aad_credentials = UserPassCredentials(self.config.get("aadUsername"), self.config.get("aadPassword"))
        self.subscription_id = self.config.get('aadSubscriptionId')
        
        #TODO: check how to simplify that since clusters are unique to Azure
        self.hdi_cluster_name = self.config.get('hdiClusterName')
        self.hdi_cluster_rg = self.config.get('hdiResourceGroup')
        
    def start(self):
        logging.info("Attaching to HDI cluster %s" % self.hdi_cluster_name)
        dss_cluster_config = dku_hdi.make_cluster_keys_and_data(self.aad_credentials, self.subscription_id, self.hdi_cluster_name, self.hdi_cluster_rg)
        return dss_cluster_config

    def stop(self, data):
        # we attach to an existing cluster so we do not stop it
        pass

