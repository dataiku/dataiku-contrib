TO_BE_REPLACED = 'TO_BE_REPLACED'

DSS_CONFIG_TMPL = [
    {'version': '5.0.1',
      'properties': { 
        'hadoop': {
            'extraConf' : {}
        },
        'hive': {
            "enabled": True,
            "hiveServer2Host" : TO_BE_REPLACED,
            "hiveServer2Port" : TO_BE_REPLACED,
            "extraUrl" : TO_BE_REPLACED,
            "useURL": True,
            "url": TO_BE_REPLACED,
            "canChart": True,
            "engineCreationSettings": {},
            "executionConfigsGenericOverrides": {}
        },
        'impala': {
            "enabled": False
        },
        'spark': {
            "sparkEnabled":  True,
            "executionConfigsGenericOverrides" : {}
        }
      }
    }]

DSS_CLUSTER_TMPL = [{
    'hadoop': {    
        "fs.defaultFS": {"target": "core-site"}, 
        #Storage config is duplicated dynamically in model
        #fs.azure.account.keyprovider.<BLOBNAME>.blob.core.windows.net=org.apache.hadoop.fs.azure.SimpleKeyProvider
        #fs.azure.account.key.<BLOBNAME>.blob.core.windows.net= <decrypted key>
        "ha.zookeeper.quorum": {"target": "core-site"},
        "yarn.resourcemanager.address": {"target": "yarn-site"},
        "yarn.resourcemanager.address.rm1": {"target": "yarn-site"},
        "yarn.resourcemanager.address.rm2": {"target": "yarn-site"},
        "yarn.resourcemanager.hostname": {"target": "yarn-site"},
        "yarn.resourcemanager.hostname.rm1": {"target": "yarn-site"},
        "yarn.resourcemanager.hostname.rm2": {"target": "yarn-site"},
        "yarn.resourcemanager.resource-tracker.address": {"target": "yarn-site"},
        "yarn.resourcemanager.resource-tracker.address.rm1": {"target": "yarn-site"},
        "yarn.resourcemanager.resource-tracker.address.rm2": {"target": "yarn-site"},
        "yarn.resourcemanager.scheduler.address": {"target": "yarn-site"},
        "yarn.resourcemanager.scheduler.address.rm1": {"target": "yarn-site"},
        "yarn.resourcemanager.scheduler.address.rm2": {"target": "yarn-site"},
        "dfs.namenode.http-address.mycluster.nn1": {"target": "hdfs-site"},
        "dfs.namenode.http-address.mycluster.nn2": {"target": "hdfs-site"},
        "dfs.namenode.https-address.mycluster.nn1": {"target": "hdfs-site"},
        "dfs.namenode.https-address.mycluster.nn2": {"target": "hdfs-site"},
        "dfs.namenode.rpc-address.mycluster.nn1": {"target": "hdfs-site"},
        "dfs.namenode.rpc-address.mycluster.nn2": {"target": "hdfs-site"}
    },
    'hive': {
           #Storage config is duplicated dynamically in model
           #fs.azure.account.keyprovider.<BLOBNAME>.blob.core.windows.net=org.apache.hadoop.fs.azure.SimpleKeyProvider
           #fs.azure.account.key.<BLOBNAME>.blob.core.windows.net= <decrypted key>
           "fs.defaultFS": {"target": "core-site"},
           #HProxy check hdfs-site for NN consistency - Needs to override here
           "dfs.namenode.http-address.mycluster.nn1": {"target": "hdfs-site"},
           "dfs.namenode.http-address.mycluster.nn2": {"target": "hdfs-site"},
           "dfs.namenode.https-address.mycluster.nn1": {"target": "hdfs-site"},
           "dfs.namenode.https-address.mycluster.nn2": {"target": "hdfs-site"},
           "dfs.namenode.rpc-address.mycluster.nn1": {"target": "hdfs-site"},
           "dfs.namenode.rpc-address.mycluster.nn2": {"target": "hdfs-site"}
    },
    'spark': {
           "spark.hadoop.fs.defaultFS": {"target": "core-site", "key": "fs.defaultFS"},
           #Storage config is duplicated dynamically in model
           #spark.hadoop.fs.azure.account.keyprovider.<BLOBNAME>.blob.core.windows.net=org.apache.hadoop.fs.azure.SimpleKeyProvider
           #spark.hadoop.fs.azure.account.key.<BLOBNAME>.blob.core.windows.net= <decrypted key>
           "spark.hadoop.yarn.resourcemanager.address": {"target": "yarn-site", "key":"yarn.resourcemanager.address"},
           "spark.hadoop.yarn.resourcemanager.address.rm1": {"target": "yarn-site", "key":"yarn.resourcemanager.address.rm1"},
           "spark.hadoop.yarn.resourcemanager.address.rm2": {"target": "yarn-site", "key":"yarn.resourcemanager.address.rm2"},
           "spark.hadoop.yarn.resourcemanager.hostname": {"target": "yarn-site", "key": "yarn.resourcemanager.hostname"},
           "spark.hadoop.yarn.resourcemanager.hostname.rm1": {"target": "yarn-site", "key": "yarn.resourcemanager.hostname.rm1"},
           "spark.hadoop.yarn.resourcemanager.hostname.rm2": {"target": "yarn-site", "key": "yarn.resourcemanager.hostname.rm2"},
           "spark.hadoop.yarn.resourcemanager.resource-tracker.address": {"target": "yarn-site", "key": "yarn.resourcemanager.resource-tracker.address"},
           "spark.hadoop.yarn.resourcemanager.resource-tracker.address.rm1": {"target": "yarn-site", "key": "yarn.resourcemanager.resource-tracker.address.rm1"},
           "spark.hadoop.yarn.resourcemanager.resource-tracker.address.rm2": {"target": "yarn-site", "key": "yarn.resourcemanager.resource-tracker.address.rm2"},
           "spark.hadoop.yarn.resourcemanager.scheduler.address": {"target": "yarn-site", "key": "yarn.resourcemanager.scheduler.address"},
           "spark.hadoop.yarn.resourcemanager.scheduler.address.rm1": {"target": "yarn-site", "key": "yarn.resourcemanager.scheduler.address.rm1"},
           "spark.hadoop.yarn.resourcemanager.scheduler.address.rm2": {"target": "yarn-site", "key": "yarn.resourcemanager.scheduler.address.rm2"},
           "spark.hadoop.dfs.namenode.http-address.mycluster.nn1": {"target": "hdfs-site", "key": "dfs.namenode.http-address.mycluster.nn1"},
           "spark.hadoop.dfs.namenode.http-address.mycluster.nn2": {"target": "hdfs-site", "key": "dfs.namenode.http-address.mycluster.nn2"},
           "spark.hadoop.dfs.namenode.https-address.mycluster.nn1": {"target": "hdfs-site", "key": "dfs.namenode.https-address.mycluster.nn1"},
           "spark.hadoop.dfs.namenode.https-address.mycluster.nn2": {"target": "hdfs-site", "key": "dfs.namenode.https-address.mycluster.nn2" },
           "spark.hadoop.dfs.namenode.rpc-address.mycluster.nn1": {"target": "hdfs-site", "key": "dfs.namenode.rpc-address.mycluster.nn1"},
           "spark.hadoop.dfs.namenode.rpc-address.mycluster.nn2": {"target": "hdfs-site", "key": "dfs.namenode.rpc-address.mycluster.nn2"}
     }
}]
                            
