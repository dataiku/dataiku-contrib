import dataiku, logging, dku_dataproc
from dataiku.runnables import Runnable, ResultTable
from gce_client import DataProcClient

rt = ResultTable()
rt.add_column("node_type", "Node type", "STRING")
rt.add_column("machine_type", "Machine type", "STRING")
rt.add_column("machine_private_ip", "Private IP", "STRING")
rt.add_column("is_preemptible", "Pre-emptible VM?", "STRING")
rt.add_column("status", "Status", "STRING")

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
        master_instance_info = {"privateIpAddress" : computeClient.getInstancePrivateIP(master_instances[0]),
                                "instanceType": computeClient.getInstanceType(master_instances[0]),
                                "instanceStatus": computeClient.getInstanceStatus(master_instances[0])}
        record = []
        record.append("master")
        record.append(master_instance_info["instanceType"])
        record.append(master_instance_info["privateIpAddress"])
        record.append("NO")
        record.append(master_instance_info["instanceStatus"])
        rt.add_record(record)

        logging.info("retrieving worker instances")
        slave_instances = client.getWorkerInstances(cluster_name,clusterBody)
        print("DEBUG")
        print(str(slave_instances))
        slave_instances_info = []
        for inst in slave_instances:
            single_slave = {"privateIpAddress": computeClient.getInstancePrivateIP(inst),
                            "instanceType": computeClient.getInstanceType(inst),
                            "is_preemptible": computeClient.getInstancePreemptibility(inst),
                            "instanceStatus": computeClient.getInstanceStatus(inst)}
            slave_instances_info.append(single_slave)

        for worker_instance in slave_instances_info:
            record = []
            record.append("worker")
            record.append(worker_instance["instanceType"])
            record.append(worker_instance["privateIpAddress"])
            record.append(worker_instance["is_preemptible"])
            record.append(worker_instance["instanceStatus"])
            rt.add_record(record)

        return rt