
import os, json, logging
import dku_dataproc
from gce_client import DataProcClient
from dataiku.cluster import Cluster

# This actually belongs in the main entry point
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class MyCluster(Cluster):

    def __init__(self, cluster_id, cluster_name, config, plugin_config):
        self.cluster_name = cluster_name
        self.config = config
        self.plugin_config = plugin_config
        self.client = None
       

    def __init_client__(self):
        logging.info("loading dataproc client")
        if not self.client:
            self.client = DataProcClient(self.config["gcloudProjectId"],asumeDefaultCredentials=True)
            self.client.region = self.config.get("gcloudRegionId")
            self.client.zone = self.config.get("gcloudZoneId")
        return

    def start(self):
        self.__init_client__()
        clusterId = self.config["gcloudClusterId"]
        logging.info("Attaching to Dataproc cluster : %s" % clusterId)
        logging.info("{}")
        return dku_dataproc.make_cluster_keys_and_data(self.client, clusterId, create_user_dir=True)

    def stop(self, data):
        """
        Since we attached to an existing cluster, we don't stop it
        """
        logging.info("Detaching: nothing to do")
