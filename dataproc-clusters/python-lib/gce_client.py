import os,sys
import googleapiclient.discovery
from google.auth import compute_engine
import random
import logging
import time

# Requirements google-auth-httplib2 google-auth google-cloud 


logger = logging.getLogger('dataiku.dataproc.handler')

class AbstractGCloudClient:
    projectId = None
    zone = None
    region = None
    apiVersion = "v1"
    client = None

    def __init__ (self,service_account_details=None,service_account_file=None):

        raise NotImplementedError("This is an abstract class ")

    def authenticate(self,force=False,service_account_details=None,service_account_file=None):
        """
        :service_account_details: dict parsed from json credentials
        :service_account_file: gcloud file credentials locations
        """
        if self.client != None and not force :
            return self.client
        if service_account_details== None and service_account_file == None:
            raise ValueError(" No credentials provided ")
        elif service_account_file != None :
            with open(service_account_file, 'r') as file_obj:
                try:
                    service_account_details = json.load(file_obj)
                except ValueError as err:
                    raise err

        if service_account_details.get("type") != "service_account" :
            raise ValueError("credentials provided are not from a service account")

        return (service_account.Credentials.from_service_account_info(service_account_details))

    def getLabelsFromTags(self,tagDict):
        return {k.lower(): v for k,v in tagDict.items()}

    def isTaggableMatchesLabels(self,taggable,labels):
        assert type(labels) == dict , "Expecting dict for labels"
        if taggable.get("labels") == None :
            if (len(labels.keys()) > 0):
                return False
            else:
                return True
        else:
            for k,v in labels.items():
                if taggable.get("labels").get(k) != v:
                    return False
            return True


class DataProcClient(AbstractGCloudClient):
    imageVersion = "1.3.9-deb9"
    credentials=None
    serviceName=None
    numberOfMaster = 1
    numberOfWorker = 2
    masterType = "n1-standard-1"
    workerType = "n1-standard-1"
    def __init__ (self,project,asumeDefaultCredentials=False,service_account_details=None,service_account_file=None):
        self.apiVersion = "v1"
        self.projectId = project

        # client 
        if asumeDefaultCredentials:
            self.client = googleapiclient.discovery.build('dataproc', self.apiVersion)
        else:
            credentials = self.authenticate(service_account_details=service_account_details,
                service_account_file=service_account_file)
            self.client =googleapiclient.discovery.build('dataproc', self.apiVersion, credentials=credentials)

        return

    def exists(self,name):
        try : 
            self.__lookup__(name)
            return True
        except Exception as e:
            logger.debug(str(e))
            pass
        return False

    def run(self,name,clusterBody=None):

        if  self.exists(name):
            logger.error("Cluster {} already exists ".format(name))
            return
        clusterBody= clusterBody or self.dump(name)
        ret = self.client.projects().regions().clusters().create(
            projectId=self.projectId,
            region=self.region,
            body=clusterBody).execute()

        return ret

    def dump(self,name,labels=None,definition=None,numberOfMaster=None,numberOfWorker=None):

        numberOfWorker = numberOfWorker or self.numberOfWorker
        numberOfMaster = numberOfMaster or self.numberOfMaster

        if not labels:
            labels= {
                "started-by": "dataiku-dss",
                "source-instance" : os.environ.get("HOSTNAME")
            }
        if not definition:
            definition = {}
        if  self.exists(name):
            
            definition = self.__lookup__(name)
            
        definition.update({
                'projectId': self.projectId,
                'clusterName': name,
                'config': {
                    'gceClusterConfig': {
                        'zoneUri': self.getApiEndpoint()
                    },
                    'masterConfig': {
                        'numInstances': numberOfMaster,
                        'machineTypeUri': self.masterType 
                    },
                    'workerConfig': {
                        'numInstances': numberOfWorker,
                        'machineTypeUri': self.workerType
                    },
                    "softwareConfig": {
                      "imageVersion": self.imageVersion
                    }
                },
                "labels": labels
            })
        return definition

    def setCustomSubnetToClusterDef(self,clusterBody,networkId):
        clusterBody['config']['softwareConfig'].update({
                'subnetworkUri': networkId
            })
        return clusterBody

        

    def setLabelsToClusterDef(self,clusterBody,labelsAsDict):
        labels = self.getLabelsFromTags(labelsAsDict)
        clusterBody["labels"].update(labels)
        return labels

    def setHiveConfToClusterDef(self,clusterBody,propsAsDict):

        softwareConfig = clusterBody['config']['masterConfig'].get("softwareConfig",{})
        properties = softwareConfig.get("properties",{})
        for k,v in propsAsDict:
            properties["hive:{}".format(k)] = v
            properties["spark:{}".format(k)] = v

        softwareConfig.update({"properties": properties})
        clusterBody['config']['masterConfig'].update({
                'softwareConfig': softwareConfig
            })
        return clusterBody

    def setHiveMetastoreToGoogleSql(self,clusterBody,instanceName,projectId=None,region=None,extraProps={}):
        # Generating metadata items
        
        projectId = projectId or self.projectId
        region = region or self.region
        items = clusterBody['config'].get("metadata",{}).get("items",[])

        # hive-metastore-instance=<PROJECT_ID>:<REGION>:<INSTANCE_NAME>
        items.append({
            "key": "hive-metastore-instance", 
            "value": "{}:{}:{}".format(projectId,region,instanceName)
            })

        # This is destined to configure KMS related settings etc 
        # We may disable this for now since we don't know the behaviour with hive-cli + global metastore
        for k,v in extraProps.items():
            items.append({"key": k,"value": v})
        clusterBody['config']['metadata'].update({
                'items': items
            })
        return clusterBody  

    def waitForStatus(self,clusterName,status="RUNNING",timeout=10):
        while timeout > 0:
            clusterDict = self.__lookup__(clusterName)
            if status == str(clusterDict.get("status").get("state")):
                print("Cluster {}  is {} ".format(clusterName , status))
                return
            time.sleep(60)
            timeout -=1

        return ValueError("cluster {} couldn't reach state {} in  {} minutes ".format(clusterName,status,str(timeout)))



    def getApiEndpoint(self):
        return 'https://www.googleapis.com/compute/{}/projects/{}/zones/{}'.format(
                self.apiVersion,self.projectId, 
                self.zone
                )



    def __lookup__(self,name):
        return self.client.projects().regions().clusters().get(
            projectId=self.projectId,
            region=self.region,
            clusterName=name).execute()

    def get_state(self,name):
        return self.__lookup__(name).get("status").get("state")

    def terminate(self,name):
        if self.get_state(name) == "RUNNING":
            self.client.projects().regions().clusters().delete(
                projectId=self.projectId,
                region=self.region,
                clusterName=name).execute()
        else:
            logger.error("Cluster {} is not RUNNING ".format(name))
        return

    def scaleCluster(self,name,nomberOfWorker,numberOfSecondaryInstance=0):
        updateMask="config.worker_config.num_instances,"

        clusterBody= {
          "config":{
            "workerConfig":{
              "numInstances":nomberOfWorker
            }
          }
        }
        # '>= 0' allows to scale down preemptible workers
        if numberOfSecondaryInstance >= 0 : 
            # implement update for spot instances 
            # update mask  config.secondary_worker_config.num_instances
            # update request body
            updateMask="config.worker_config.num_instances,config.secondary_worker_config.num_instances"
            clusterBody['config']["secondaryWorkerConfig"] = {
              "numInstances": numberOfSecondaryInstance
            }
            pass



        ret = self.client.projects().regions().clusters().patch(
            projectId=self.projectId,
            region=self.region,
            updateMask=updateMask,
            clusterName=name,
            body=clusterBody).execute()
        return ret

    def listClusters(self):
        # Pooling result via API 
        return self.client.projects().regions().clusters().list(
            projectId=self.projectId,
            region=self.region).execute().get("clusters",[])


    def getMasterInstances(self,name,clusterDefinition=None):
        clusterDefinition = clusterDefinition or self.__lookup__(name)
        return clusterDefinition.get("config",{}).get("masterConfig",{})


    def getWorkerInstances(self,name,clusterDefinition=None):
        clusterDefinition = clusterDefinition or  self.__lookup__(name)
        return clusterDefinition.get("config",{}).get("workerConfig",{}).get("instanceNames",[])



    def getChildInstances(self,name,clusterDefinition=None):
        clusterDefinition = clusterDefinition or self.__lookup__(name)
        ret = self.getMasterInstances(name,clusterDefinition=clusterDefinition) 
        ret.extend(self.getWorkerInstances(name,clusterDefinition=clusterDefinition))
        return ret

        
    def setCustomHadoopProps(self,clusterDefinition,props):
        assert type(props) == dict , "Wrong properties input for {} ".format(name)
        clusterDefinition.get("config").get("masterConfig").get("properties").update(props)
        return

    def getDataprocClusterByName(self,name):
        return self.__lookup__(name)
    def forkComputeClient(self):
        computeClient = GcloudComputeClient(asumeDefaultCredentials=True)
        computeClient.region = self.region
        computeClient.zone = self.zone
        computeClient.projectId = self.projectId
        return computeClient

    __definition = None




class GcloudComputeClient(AbstractGCloudClient):

    def __init__(self,asumeDefaultCredentials=False,service_account_details=None,service_account_file=None):
        """
        :asumeDefaultCredentials: if False the service will not authenticate
        """
        # client 
        if asumeDefaultCredentials:
            self.client = googleapiclient.discovery.build('compute', self.apiVersion)
        else:
            credentials = self.authenticate(service_account_details=service_account_details,
                service_account_file=service_account_file)
            self.client =googleapiclient.discovery.build('compute', self.apiVersion, credentials=credentials)

        return


    def get_instance(self,name,zone=None,region=None):
        zone = zone or self.zone
        region = region or self.region 
        return self.client.instances().get(project=self.projectId,zone=zone,instance=name).execute()

    def getInstanceByName(self,name,tagDict={},owner=None,zone=None,region=None):
        instance = self.get_instance(name,zone,region)
        if self.isIntanceMatchesLabels(instance,self.getLabelsFromTags(tagDict)): 
            return instance
        else:
            return None

    def getInstancePublicIP(self,name,instance=None,zone=None,region=None):
        # Performance optimization around instance already having  descriptor in cache  
        instance = instance or self.get_instance(name,zone=zone,region=region)
        for iface in instance.get("networkInterfaces"):
            for access in iface.get("accessConfigs"):
                if access.get("natIP") != None:
                    return access.get("natIP")
        return None

    def getInstancePrivateIP(self,name,instance=None,zone=None,region=None):
        # Performance optimization around instance already having  descriptor in cache  
        instance = instance or self.get_instance(name,zone=zone,region=region)
        for iface in instance.get("networkInterfaces"):
            return str(iface.get("networkIP"))
        return None

    def getInstanceType(self, name, instance=None, zone=None, region=None):
        # For using info directly from "native" GCP client to fetch machine type for macros
        instance = instance or self.get_instance(name, zone=zone, region=region)
        instance_type = instance.get("machineType").split("/")[-1]
        return str(instance_type)

    def getInstancePreemptibility(self, name, instance=None, zone=None, region=None):
        # For using info directly from "native" GCP client to fetch machine type for macros
        instance = instance or self.get_instance(name, zone=zone, region=region)
        is_preemptible_instance = ("YES" if instance["scheduling"]["preemptible"] else "NO")
        return is_preemptible_instance
        

    def isIntanceMatchesLabels(self,instance,labels):
        return self.isTaggableMatchesLabels(instance,labels)


    def getInstanceStatus(self,instance_name):
        return self.get_instance(instance_name).get("status")



        


class DataprocPluginError(Exception):
    pass


