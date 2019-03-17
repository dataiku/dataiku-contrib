import dataiku, logging, dku_dataproc
from dataiku.runnables import Runnable
from gce_client import DataProcClient


logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        dss_cluster = dataiku.api_client().get_cluster(self.config["dss_cluster_id"])
        settings = dss_cluster.get_settings()
        (client, cluster_name) = dku_dataproc.get_client_and_wait(settings)
        computeClient = client.forkComputeClient()
        clusterBody = client.getDataprocClusterByName(cluster_name)

        logging.info("retrieving master instance")
        master_instances = client.getMasterInstances(cluster_name,clusterBody).get("instanceNames")
        master_instance_info = {"privateIpAddress" : computeClient.getInstancePrivateIP(master_instances[0])}

        logging.info("retrieving worker instances")
        slave_instances = client.getWorkerInstances(cluster_name,clusterBody)
        slave_instances_info = [
                {"privateIpAddress":  computeClient.getInstancePrivateIP(inst)} for inst in slave_instances]    

        return {
            "masterInstance" : master_instance_info,
            "slaveInstances":  slave_instances_info,
            'config': clusterBody.get("config")
               }
        