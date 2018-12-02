import copy
import json
import logging
import os
import pwd
import requests
import subprocess
import traceback

class HdiAmbariClient(object):
    def __init__(self, host, username, password, identifier=None, validate_ssl=True,
                 timeout=10, max_retries=5):

        if not identifier:
            identifier = 'Ambari'
            
        self.request_params = {
            'headers': {'X-Requested-By': identifier},
            'auth': (username, password),
            'verify': validate_ssl,
            'timeout': timeout,
        }
        # automatically retry requests on connection errors
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(max_retries=max_retries)
        self.session.mount(host, adapter)
        self.host = host
        #TODO: always refresh configs tags here
        self.configs_tags = None
    
    def get_desired_configs_tags(self, cluster_name):
        #TODO: explain here why we do that
        url_meta = self.host + '/api/v1/clusters/{cluster_name}?fields=Clusters/desired_configs'
        url = url_meta.format(cluster_name=cluster_name)
        resp = self.session.get(url, **self.request_params)
        return resp.json()['Clusters']['desired_configs']
    
    def set_desired_configs_tags(self, cluster_name):
        self.configs_tags = self.get_desired_configs_tags(cluster_name)
        return self.configs_tags
    
    def get_config(self, cluster_name, config_name='core-site'):
        url_meta = self.host + '/api/v1/clusters/{cluster_name}/configurations?type={config_name}&tag={config_tag}'
        if config_name not in self.configs_tags.keys():
            raise ValueError('Configuration requested {} does not exist in cluster'.format(config_name))
        
        config_tag = self.configs_tags[config_name]['tag']
        url = url_meta.format(cluster_name=cluster_name, config_name=config_name, config_tag=config_tag)
        
        resp = self.session.get(url, **self.request_params)
        return resp.json()['items'][0]['properties']
    
    
    def get_services_config(self, services_list):
        url_meta = self.host + '/api/v1/clusters/{cluster_name}/configurations/service_config_versions?service_name.in({services_list})&is_current=true'
        url = url_meta.format(cluster_name=cluster_name, services_list=services_list)
        resp = self.session.get(url, **self.request_params)
        return resp.json()
        