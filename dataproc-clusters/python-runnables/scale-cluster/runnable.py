import dataiku, logging, dku_dataproc
from dataiku.runnables import Runnable

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
        (client, cluster_id) = dku_dataproc.get_client_and_wait(settings)

        client.scaleCluster(cluster_id,self.config["regular_worker_instances"],
            numberOfSecondaryInstance=self.config["spot_worker_instances"])
        
        return "Done"