import logging, subprocess, os, pwd, copy, json
from gce_client import DataProcClient,GcloudComputeClient





def get_client_and_wait(started_cluster_settings):
    """
    Get a  client from the :class:`dataikuapi.dss.admin.DSSClusterSettings` of a started DSS cluster

    :returns: tuple (dataproc client, dataproc cluster id)
    """
    

    config = started_cluster_settings.get_raw()["params"]["config"]
    data = started_cluster_settings.get_plugin_data()
    if data is None:
        raise ValueError("No cluster data, is it stopped/detached?")

    logging.info("creating client for cluster, region=%s",config["gcloudRegionId"],config["gcloudRegionId"])

    client = DataProcClient(config["gcloudProjectId"],asumeDefaultCredentials=True)
    client.region = config.get("gcloudRegionId")
    client.zone = config.get("gcloudZoneId")

    cluster_id = data["dataprocClusterId"]
    logging.info("waiting for cluster %s to be running" % cluster_id)
    client.waitForStatus(cluster_id,status='RUNNING')
    logging.info("cluster started")
    return (client, cluster_id)

def make_cluster_keys_and_data(client, cluster_id,clusterBody=None, create_user_dir=False, create_databases=None):
    computeClient = client.forkComputeClient()
    logging.info("looking up cluster %s" % cluster_id)
    logging.info("client init region {} zone {}".format(client.region,client.zone))
    clusterBody = client.__lookup__(cluster_id)

    masterInstances = client.getMasterInstances(cluster_id,clusterBody).get("instanceNames")

    masterDef = None
    master_ip = None
    for instanceName in masterInstances:
        masterDef = computeClient.get_instance(instanceName)
        if masterDef :
            master_ip = computeClient.getInstancePrivateIP(instanceName,instance=masterDef)
            break

    if not masterDef : 
        raise ValueError("could not get Master definition")


    hadoop_keys = {
        "extraConf" : [
           {"key": "fs.defaultFS", "value" : "hdfs://%s/" % master_ip},
           {"key": "yarn.resourcemanager.address" , "value" :  "%s:8032" % master_ip},
           {"key": "yarn.resourcemanager.hostname" , "value" :  master_ip },
           {"key": "yarn.resourcemanager.scheduler.address" , "value" :  "%s:8030" % master_ip},
           {"key": "yarn.timeline-service.hostname" , "value" :  "%s" % master_ip},
           {"key": "yarn.timeline-service.webapp.address" , "value" :  "%s:8188" % master_ip},
           {"key": "mapreduce.jobhistory.address" , "value" :  "%s:10020" % master_ip},
           {"key": "fs.gs.project.id" , "value" :  "%s" % client.projectId},  
           {"key": "fs.AbstractFileSystem.gs.impl" , "value" :  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"}

        ]
    }
    hive_keys = {
        "enabled": True,
        "hiveServer2Host" : master_ip,
        "executionConfigsGenericOverrides" : [
           {"key": "fs.defaultFS", "value" : "hdfs://%s:8020" % master_ip},
           {"key": "yarn.resourcemanager.address" , "value" :  "%s:8032" % master_ip},
           {"key": "yarn.resourcemanager.scheduler.address" , "value" :  "%s:8030" % master_ip},
           {"key": "yarn.timeline-service.hostname" , "value" :  "%s" % master_ip},
           {"key": "mapreduce.jobhistory.address" , "value" :  "%s:10020" % master_ip},
           {"key": "hive.metastore.uris", "value": "thrift://%s:9083" % master_ip}
        ]
    }
    impala_keys = {
        "enabled": False
    }
    spark_keys = {
        "sparkEnabled":  True,
        "executionConfigsGenericOverrides" : [
           {"key": "spark.hadoop.fs.defaultFS", "value" : "hdfs://%s:8020" % master_ip},
           {"key": "spark.hadoop.yarn.resourcemanager.address" , "value" :  "%s:8032" % master_ip},
           {"key": "spark.hadoop.yarn.resourcemanager.scheduler.address" , "value" :  "%s:8030" % master_ip},
           {"key": "spark.hadoop.hive.metastore.uris" , "value" :  "thrift://%s:9083" % master_ip},
           {"key": "spark.yarn.historyServer.address" , "value" :  "%s:18080" % master_ip },
           {"key": "spark.eventLog.dir" , "value" :  "hdfs://%s/user/spark/eventlog" % master_ip },
           {"key": "spark.history.fs.logDirectory" , "value" :  "hdfs://%s/user/spark/eventlog" % master_ip }
           
        ]
    }

    if create_user_dir:
        username = pwd.getpwuid(os.geteuid()).pw_name
        homedir = "hdfs://%s:8020/user/%s" % (master_ip, username)
        logging.info("creating home directory %s" % homedir)
        env = copy.deepcopy(os.environ)
        # Group 'hadoop' is in dfs.permissions.superusergroup on EMR
        env["HADOOP_USER_NAME"] = "hadoop"
        subprocess.check_call(["hdfs", "dfs", "-mkdir", "-p", homedir], env=env)

    if create_databases:
        dbs = [ db.strip() for db in create_databases.split(',') if db.strip() ]
        if dbs:
            logging.info("creating hive databases %s" % dbs)
            subprocess.check_call(["beeline", "-u", "jdbc:hive2://%s:10000" % master_ip, "-e",
                    ' '.join([ 'create database if not exists `%s`;' % db for db in dbs ])
                ])
                
    logging.info("done attaching cluster")

    return [{'hadoop':hadoop_keys, 'hive':hive_keys, 'impala':impala_keys, 'spark':spark_keys}, {
        "dataprocClusterId":  cluster_id
    }]
