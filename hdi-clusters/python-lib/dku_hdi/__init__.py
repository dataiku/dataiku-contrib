import copy
import json
import logging
import os
import pwd
import requests
import subprocess
import traceback
from pprint import pprint, pformat

from azure.mgmt.hdinsight import HDInsightManagementClient
from .ambari.client import HdiAmbariClient
from .models import AbstractDSSConfigBuilder

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)


def make_cluster_keys_and_data(aad_credentials, subscription_id, hdi_cluster_name, hdi_cluster_rg):
    # aad_credentials of type azure.common.credentials.InteractiveCredentials, ServicePrincipalCredentials, UserPassCredentials
    hdi_client = HDInsightManagementClient(aad_credentials, subscription_id)
    cluster = hdi_client.clusters.get(hdi_cluster_rg, hdi_cluster_name)
    cluster_core_info = hdi_client.configurations.get(hdi_cluster_rg, hdi_cluster_name, 'core-site')
    logging.info('HDI client retreived core info {}'.format(pformat(cluster_core_info)))
    
    cluster_gateway = hdi_client.configurations.get(hdi_cluster_rg, hdi_cluster_name, 'gateway')    
    try:
        ambari_user = cluster_gateway['restAuthCredential.username']
        ambari_pwd = cluster_gateway['restAuthCredential.password']
    except KeyError:
        logging.error('Could not retreive ambari gateway credentials')
        raise

    cluster_endpoints = cluster.properties.connectivity_endpoints
    ambari_host = ['https://' + e.location for e in cluster_endpoints if e.port == 443 and e.name == 'HTTPS'][0]

    ambari_client = HdiAmbariClient(ambari_host, ambari_user, ambari_pwd)
    conf_tags = ambari_client.set_desired_configs_tags(hdi_cluster_name)
    logging.info('Updated config tags:\n {}'.format(pformat(conf_tags)))

    dss_config_builder = AbstractDSSConfigBuilder(hdi_cluster_name, ambari_client)
    storage_info = dss_config_builder.make_storage_from_hdi_core_info(cluster_core_info)
    dss_config = dss_config_builder.make_dss_config(storage_info)
    logging.info('Returning DSS cluster config {}'.format(pformat(dss_config)))

    return [dss_config, {'hdiClusterId': hdi_cluster_name, 'subscriptionId': subscription_id, 'resourceGroupName': hdi_cluster_rg}]
