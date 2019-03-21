import dku_dataproc
from gce_client import DataProcClient
import os, json, argparse, logging
from dataiku.cluster import Cluster

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


class MyCluster(Cluster):
    def __init__(self,cluster_id, cluster_name,config, plugin_config):
        self.cluster_name = cluster_name
        self.config = config
        self.plugin_config = plugin_config
        self.client = None
        self.cluster_id = config.get("gcloudClusterId") or cluster_id
        return

    def __init_client__(self):
        logging.info("loading dataproc client")
        if not self.client:
            self.client = DataProcClient(self.config["gcloudProjectId"],asumeDefaultCredentials=True)
            self.client.region = self.config.get("gcloudRegionId")
            self.client.zone = self.config.get("gcloudZoneId")

        return
        
    def start(self):
        logging.info("starting cluster")
        self.__init_client__()
        full_name = "DSS cluster name=%s" % (self.cluster_name)
        name = self.cluster_id
        logging.info("starting cluster, name=%s" % (full_name))

        Tags={}
        for tag in self.config.get("tags", []):
            Tags[tag["from"]] = tag["to"]
        clusterBody = self.client.dump(name)
        self.client.dump(name)

        clusterBody["config"]["masterConfig"]['machineTypeUri'] = self.config.get("masterInstanceType")
        clusterBody["config"]["workerConfig"]['machineTypeUri'] = self.config["slaveInstanceType"]
        clusterBody["config"]["workerConfig"]['numInstances'] = int(self.config["instancesCount"])
                
        

        props = {}
        if self.config["metastoreDBMode"] == "CUSTOM_JDBC":
            props = {
                "javax.jdo.option.ConnectionURL" : self.config["metastoreJDBCURL"],
                "javax.jdo.option.ConnectionDriverName": self.config["metastoreJDBCDriver"],
                "javax.jdo.option.ConnectionUserName": self.config["metastoreJDBCUser"],
                "javax.jdo.option.ConnectionPassword": self.config["metastoreJDBCPassword"],
            }
            logging.imfo(" setting hive metastore for custom JDBC")
            self.client.setHiveConfToClusterDef(clusterBody,props)
        elif self.config["metastoreDBMode"] == "MYSQL":
            props = {
                "javax.jdo.option.ConnectionURL" : "jdbc:mysql://%s:3306/hive?createDatabaseIfNotExist=true" % self.config["metastoreMySQLHost"],
                "javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
                "javax.jdo.option.ConnectionUserName": self.config["metastoreMySQLUser"],
                "javax.jdo.option.ConnectionPassword": self.config["metastoreMySQLPassword"]
            }
            logging.imfo(" setting hive metastore for MySQL")
            self.client.setHiveConfToClusterDef(clusterBody,props)
        elif self.config["metastoreDBMode"] == "GCLOUD_SQL":
            self.client.setHiveMetastoreToGoogleSql(clusterBody,instanceName,projectId=None,region=None,extraProps={})
                

        # Building Dataproc cluster
        self.client.run(name,clusterBody)

        logging.info("waiting for cluster to start")
        self.client.waitForStatus(name)
        
        return dku_dataproc.make_cluster_keys_and_data(self.client, self.cluster_id,clusterBody=clusterBody, create_user_dir=True, create_databases=self.config.get("databasesToCreate"))

    def stop(self, data):
        """
        Stop the cluster
        
        :param data: the dict of data that the start() method produced for the cluster
        """
        logging.info("Deleting cluster {}".format(self.cluster_id))
        self.__init_client__()
        self.client.terminate(data["dataprocClusterId"])
        return
