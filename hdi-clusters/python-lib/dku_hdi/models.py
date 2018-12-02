import copy
import logging
from collections import OrderedDict
from pprint import pprint
from urlparse import urlparse, urlsplit

from .templates import DSS_CONFIG_TMPL, DSS_CLUSTER_TMPL

class DSSConfigBuilderClassFactory(object):
    #TODO: to be completed
    @classmethod
    def make_dss_config_builder_class(cls):
        class DSSConfigBuilder(AstractDSSConfigBuilder):
            def __init__(self, cluster_name, ambari_client):
                super().__init__(self, cluster_name, ambari_client)
                
        return DSSConfigBuilder

class AbstractDSSConfigBuilder(object):
    
    #TODO: generate this dynamically from templates ?
    REQUIRED_CONFIGS_FILENAMES = ['core-site', 'hdfs-site', 'yarn-site', 'hive-site']
    #TODO: should we add a configurable DB ?
    HS2_URL_ZK_META = 'jdbc:hive2://{}/dataiku;transportMode=http;httpPath=cliservice;serviceDiscoveryMode=zooKeeper'
    WASB_ENCRYPT_KEY_PROVIDER_META = 'fs.azure.account.keyprovider.{blob_fqdn}'
    
    cluster_name = None
    ambari_client = None
    conf_tags = None
    required_configs = {}
    
    dss_config = None
    dss_cluster_template = None
    
    
    def __init__(self, cluster_name, ambari_client):
        
        self.ambari_client = ambari_client
        self.cluster_name = cluster_name
        
        #TODO: did this to further change template depending on versions
        self.dss_config = DSS_CONFIG_TMPL[0]['properties']
        #TODO: this should be moved to an abstract method
        #FIXME: Default points at HDI gateway, won't work without SSL management - overriden by internal ZK access
        self.dss_config['hive']['hiveServer2Host'] = '{cluster_name}.azurehdinsight.net'.format(cluster_name=cluster_name)
        self.dss_config['hive']['hiveServer2Port'] = 443
        self.dss_config['hive']['extraUrl'] = 'transportMode=http'
        self.dss_cluster_template = DSS_CLUSTER_TMPL[0]
        
        #FIXME: should not be necessary? as already handled by ambari client... may keep if reading from files in future though...
        self.update_config_versions()
        
        #TODO: to be moved fully to a method so that init can be overriden (be aware of referencing the template)
        logging.info('Overriding HiveServer2 config for Zookeeper')
        self.dss_config['hive']['useURL'] = True
        self.dss_config['hive']['url'] = self.make_hs2_url_from_zk(cluster_name)
        #Force HS2 execution engine because HiveCLI is problematic with encrypted wasb keys
        self.dss_config['hive']['engineCreationSettings']['executionEngine'] = 'HIVESERVER2'
    
        
    def update_config_versions(self):
        self.conf_tags = self.ambari_client.set_desired_configs_tags(self.cluster_name)
        logging.info('Using conf tags %s' % self.conf_tags)
        for c_name in self.REQUIRED_CONFIGS_FILENAMES:
            print('required configs filenames %s'% self.REQUIRED_CONFIGS_FILENAMES)
            self.required_configs[c_name] = self.ambari_client.get_config(self.cluster_name, config_name=c_name)
        
        return self.required_configs.keys()
    
    def make_hs2_url_from_zk(self, cluster_name):
        zk_endpoints = self.required_configs['core-site']['ha.zookeeper.quorum']
        return self.HS2_URL_ZK_META.format(zk_endpoints)
    
    def make_storage_from_hdi_core_info(self, hdi_core_info):
        if not hdi_core_info.get('fs.defaultFS', None):
            raise Exception('hdi_core_info does not contain default FS or is not correct {}'.format(hdi_core_info))
        
        default_fs = hdi_core_info['fs.defaultFS']
        rsp = urlsplit(default_fs)
        
        #TODO: put a real storage representation one day...
        if rsp.scheme == 'wasb':
            blob_fqdn = rsp.hostname
            logging.info('Detected default wasb storage {}'.format(blob_fqdn))
        else:
            raise ValueError('Detected unsupported storage type: {}'.format(rsp.scheme))
        
        #Adding default keyprovider to Simple type, if not ShellProvider will be used on workers and generate errors
        wasb_encrypt_key_provider = self.WASB_ENCRYPT_KEY_PROVIDER_META.format(blob_fqdn=blob_fqdn)
        hdi_core_info[wasb_encrypt_key_provider] = 'org.apache.hadoop.fs.azure.SimpleKeyProvider'
        spark_conf = {}
        for k in hdi_core_info.keys():
            spark_conf['spark.hadoop.{storage_key}'.format(storage_key=k)] = hdi_core_info[k]
        
        return {'hadoop': hdi_core_info,
                #Need to duplicate WASB storage config in Hive if not it will not be seen by HProxy
                'hive': hdi_core_info,
                'spark': spark_conf}
        
    
    #TODO: this is probably to be overwritten when templates change, keep it here for now
    def make_dss_config(self, add_overwrite_keys=None):
        dss_services_config = self.generate_dss_services_config(add_overwrite_keys)
        dss_config = copy.deepcopy(self.dss_config)
        dss_config['hadoop']['extraConf'] = dss_services_config['hadoop']
        dss_config['hive']["executionConfigsGenericOverrides"] = dss_services_config['hive']
        dss_config['spark']["executionConfigsGenericOverrides"] = dss_services_config['spark']
        
        return dss_config
        
    
    def generate_dss_services_config(self, add_overwrite_keys=None):
        #overwrite keys in the form {'service_name: {key_value_dict}}
        dss_services_config = {}
        for service in self.dss_cluster_template:
            if add_overwrite_keys.get(service, None):
                logging.info('Found overriding keys for service{}'.format(service))
            service_keys = copy.deepcopy(self.dss_cluster_template[service])
            service_keys = self.make_extra_conf(service_keys, add_overwrite_keys.get(service, None))
            dss_services_config[service] = service_keys
        
        return dss_services_config
    
    
    def make_extra_conf(self, extra_conf_template, add_overwrite_keys=None):
        for k in extra_conf_template:
            value = self._generate_value_from_template_key(k, extra_conf_template[k])
            if value:
                extra_conf_template[k] = value
            else:
                logging.warn('Value for key %s is None removing from config' % k)
                extra_conf_template.pop(k)
        
        # Useful to build storage config - keep it after first template transformation
        if add_overwrite_keys:
            for k in add_overwrite_keys:
                new_value = add_overwrite_keys[k]
                if extra_conf_template.get(k):
                    logging.warn('Overriding key:{} value: {} with new_value {}'.format(k,extra_conf_template[k], new_value))
                else:
                    logging.info('Adding new key:{} with value: {}'.format(k, new_value))
                
                extra_conf_template[k] = new_value
        
        return self._make_extra_conf_as_kv_list(extra_conf_template)
        
        
        
    def _make_extra_conf_as_kv_list(self, key_dict):
        #Takes a single key dict and convert it to a list with format {'key': '', 'value':''} for DSS
        return [{'key': k , 'value': v} for k,v in key_dict.iteritems()]
    
    def _generate_value_from_template_key(self, key, target_dict):
        target = target_dict.get('target', None)
        value_key = target_dict.get('key', key)
        value_default = target_dict.get('value', None)
        
        if not target:
            if value_default:
                logging.info('Returning default value {}'.format(value_default))
                return value_default
            else:
                raise ValueError('Target is None with no default value')
                
        return self.get_kv_from_conf(target, value_key)
    
    def get_kv_from_conf(self, conf_file_name, conf_key):
        # cluster config file name as it is returned by ambari e.g core-site, hdfs-site etc.
        if not self.required_configs.get(conf_file_name, None):
            raise ValueError('Target file {} does not exist in downloaded configurations'.format(conf_file_name))
        
        try:
            ret_value = self.required_configs[conf_file_name][conf_key]
        except:
            logging.error('Required key {} does not exist in target file {}, dumping all configuration present'.format(conf_key, conf_file_name))
            logging.error(self.required_configs)
            raise
        
        return ret_value

