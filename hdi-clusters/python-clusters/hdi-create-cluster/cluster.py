import os, json, logging
from pprint import pprint, pformat

from dataiku.cluster import Cluster
import dku_hdi

import azure.mgmt.hdinsight
from azure.mgmt.hdinsight import HDInsightManagementClient
#Raise errors TODO why ? probably a python3 compatibility issue
#from azure.mgmt.hdinsight.models import *
from azure.mgmt.hdinsight.models import ( 
    ClusterDefinition,
    SecurityProfile,
    HardwareProfile,
    VirtualNetworkProfile,
    DataDisksGroups,
    SshPublicKey,
    SshProfile,
    LinuxOperatingSystemProfile,
    OsProfile,
    ScriptAction,
    Role,
    ComputeProfile,
    StorageAccount,
    StorageProfile,
    ClusterCreateProperties,
    ClusterCreateParametersExtended,
    ClusterPatchParameters,
    QuotaInfo,
    Errors,
    ConnectivityEndpoint,
    ClusterGetProperties,
    #TODO: Check why it raises an inspect error at import
    #Cluster,
    RuntimeScriptAction,
    ExecuteScriptActionParameters,
    ClusterListPersistedScriptActionsResult,
    ScriptActionExecutionSummary,
    RuntimeScriptActionDetail,
    ClusterListRuntimeScriptActionDetailResult,
    ClusterResizeParameters,
    OperationResource,
    Resource,
    TrackedResource,
    ProxyResource,
    ErrorResponse, ErrorResponseException,
    ApplicationGetHttpsEndpoint,
    ApplicationGetEndpoint,
    ApplicationProperties,
    Application,
    VersionSpec,
    VersionsCapability,
    RegionsCapability,
    VmSizesCapability,
    VmSizeCompatibilityFilter,
    RegionalQuotaCapability,
    QuotaCapability,
    CapabilitiesResult,
    LocalizedName,
    Usage,
    UsagesListResult,
    Extension,
    ClusterMonitoringResponse,
    ClusterMonitoringRequest,
    ScriptActionPersistedGetResponseSpec,
    OperationDisplay,
    Operation,
    ClusterPaged,
    ApplicationPaged,
    RuntimeScriptActionDetailPaged,
    OperationPaged,
    DirectoryType,
    OSType,
    Tier,
    HDInsightClusterProvisioningState,
    AsyncOperationState
)

from azure.common.credentials import InteractiveCredentials, ServicePrincipalCredentials, UserPassCredentials

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

class MyCluster(Cluster):
    def __init__(self, cluster_id, cluster_name, config, plugin_config):
        """
        :param cluster_id: the DSS identifier for this instance of cluster
        :param cluster_name: the name given by the user to this cluster
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.cluster_id = cluster_id
        self.dss_cluster_name = cluster_name
        self.config = config
        self.plugin_config = plugin_config
        
        self.aad_client_credentials = None
        
        #TODO: check when credentials are not the right way or incorrect
        if config['aadAuth'] == "user_pass":
            print("Using User Password authentication")
            self.aad_username = config['aad_username']
            self.aad_password = config['aad_password']
            self.aad_client_credentials = UserPassCredentials(username=self.aad_username, password=self.aad_password)
        elif config['aadAuth'] == "service_principal":
            print('Using Service Principal for authentication')
            self.client_id = config['client_id']
            self.client_secret = config['client_secret']
            self.tenant_id = config['tenant_id']
            self.aad_client_credentials = ServicePrincipalCredentials(self.client_id, self.client_secret, tenant=self.tenant_id)
        else:
            raise ValueError('Unsupported authentication method')
        
        #params
        self.subscription_id = config['subscription_id']
        self.cluster_version = config['cluster_version']
        self.hdi_cluster_name = config['basename']
        self.resource_group_name = config['resource_group_name']
        self.location = config['location']
        #TODO: should retreive available formats for output in case of error?
        self.headnode_size = config['headnode_size']
        self.worker_size = config['worker_size']
        self.worker_count = int(config['worker_count'])
        self.gateway_username = config['gateway_username']
        self.gateway_password = config['gateway_password']
        self.ssh_username = config['ssh_username']
        #TODO: implement ssh with uploaded key
        self.ssh_password = config['ssh_password']
        
        self.storage_account_name = '{}.blob.core.windows.net'.format(config['storage_account'])
        self.storage_account_key = config['storage_account_key']
        self.storage_account_container = config['storage_account_container']
        
        self.vnet_name = config['vnet_name']
        self.subnet_name = config['subnet_name']
        self.vnet_id = '/subscriptions/{subsId}/resourceGroups/{rgName}/providers/Microsoft.Network/virtualNetworks/{vnetName}'.format(
            subsId=self.subscription_id,
            rgName=self.resource_group_name,
            vnetName=self.vnet_name)
        self.subnet_id = '/subscriptions/{subsId}/resourceGroups/{rgName}/providers/Microsoft.Network/virtualNetworks/{vnetName}/subnets/{subnetName}'.format(
            subsId=self.subscription_id,
            rgName=self.resource_group_name,
            vnetName=self.vnet_name,
            subnetName=self.subnet_name)
        self.vnet_profile = VirtualNetworkProfile(
            id=self.vnet_id,
            subnet=self.subnet_id
        )
        
        #TODO: better test the subscription_id here ?
        self.hdi_client = HDInsightManagementClient(self.aad_client_credentials, self.subscription_id)
        
    def start(self):
        """
        Make the cluster operational in DSS, creating an actual cluster if necessary.
        
        :returns: a tuple of : 
                  * the settings needed to access hadoop/hive/impala/spark on the cluster. If not
                    specified, then the corresponding element (hadoop/hive/impala/spark) is not overriden
                  * an dict of data to pass to to other methods when handling the cluster created
        """        
        logging.info("Init cluster for HDI")
        
        create_params = ClusterCreateParametersExtended(
            location=self.location,
            tags={},
            properties=ClusterCreateProperties(
                #TODO: parametrize this correctly
                cluster_version="3.6",
                os_type=OSType.linux,
                tier=Tier.standard,
                cluster_definition=ClusterDefinition(
                    kind="spark",
                    configurations={
                        "gateway": {
                            "restAuthCredential.enabled_credential": "True",
                            "restAuthCredential.username": self.gateway_username,
                            "restAuthCredential.password": self.gateway_password
                        }
                    }
                ),
                compute_profile=ComputeProfile(
                    roles=[
                        Role(
                            name="headnode",
                            target_instance_count=2,
                            hardware_profile=HardwareProfile(vm_size=self.headnode_size),
                            os_profile=OsProfile(
                                linux_operating_system_profile=LinuxOperatingSystemProfile(
                                    username=self.ssh_username,
                                    password=self.ssh_password
                                )
                            ),
                            virtual_network_profile=self.vnet_profile
                        ),
                        Role(
                            name="workernode",
                            target_instance_count=self.worker_count,
                            hardware_profile=HardwareProfile(vm_size=self.worker_size),
                            os_profile=OsProfile(
                                linux_operating_system_profile=LinuxOperatingSystemProfile(
                                    username=self.ssh_username,
                                    password=self.ssh_password
                                )
                            ),
                            virtual_network_profile=self.vnet_profile
                        )
                    ]
                ),
                storage_profile=StorageProfile(
                    storageaccounts=[StorageAccount(
                        name=self.storage_account_name,
                        key=self.storage_account_key,
                        container=self.storage_account_container,
                        is_default=True
                    )]
                )
            )
        )

        logging.info('Creating Cluster ....')
        create_poller = self.hdi_client.clusters.create(self.resource_group_name, self.hdi_cluster_name, create_params)
        logging.info('Waiting for result poller...')
        try:
            cluster = create_poller.result()
        except:
            logging.error('Cluster creation failed, deleting what was provisioned')
            try:
                self.hdi_client.clusters.delete(self.resource_group_name, self.hdi_cluster_name)
            except:
                logging.error('Could not delete provisioned resources')
                pass
            raise
            
        logging.info('Poller resturned {}'.format(pformat(cluster)))
        
        try:
            dss_cluster_config = dku_hdi.make_cluster_keys_and_data(self.aad_client_credentials, self.subscription_id, self.hdi_cluster_name, self.resource_group_name)
        except:
            logging.error('Could not attach to created cluster, deleting')
            try:
                self.hdi_client.clusters.delete(self.resource_group_name, self.hdi_cluster_name)
            except:
                logging.error('Could not delete created cluster')
                pass
            raise
            
        return dss_cluster_config

    def stop(self, data):
        """
        Stop the cluster
        
        :param data: the dict of data that the start() method produced for the cluster
        """
        logging.info('Trying to delete remote cluster {hdiName} in resource group {rgName}'.format(
            hdiName=self.hdi_cluster_name,
            rgName=self.resource_group_name))
        try:
            delete_poller = self.hdi_client.clusters.delete(self.resource_group_name, self.hdi_cluster_name)
            ret = delete_poller.result()
        except:
            raise
        
        logging.info('Cluster deleted successfully returned {}'.format(ret))
        
        return
   
