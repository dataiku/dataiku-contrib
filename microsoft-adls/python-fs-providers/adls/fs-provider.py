import os, json
import shutil
from dataiku.fsprovider import FSProvider
from azure.datalake.store import core, lib

class ADLSFSProvider(FSProvider):
    
    def __init__(self, root, config, plugin_config):
        self.root = root
        self.root_lnt = self.get_lnt_path(root)
        self.client_id = config["client-id"]
        self.client_secret = config["client-secret"]
        self.tenant_id = config["tenant-id"]
        self.resource = "https://datalake.azure.net/"
        self.adls_account = config["adls-account"]
        
        self.adls_creds = lib.auth(
            resource = self.resource,
            tenant_id = self.tenant_id,
            client_id = self.client_id,
            client_secret = self.client_secret,
            api_version = None
        )
        self.adls_client = core.AzureDLFileSystem(self.adls_creds, store_name=self.adls_account)
        
    def get_adls_lnt_path(self, path):
        lnt_path = self.get_lnt_path(path)
        if lnt_path == '/':
            return self.root_lnt
        else:
            return self.root_lnt + lnt_path
        
    def get_lnt_path(self, path):
        if len(path) == 0 or path == '/':
            return '/'
        elts = path.split('/')
        elts = [e for e in elts if len(e) > 0]
        return '/' + '/'.join(elts)

    def browse(self, path):
        adls_path = self.get_adls_lnt_path(path)
        path_lnt = self.get_lnt_path(path)
        if not self.adls_client.exists(adls_path):
            return {'fullPath' : path_lnt, 'exists' : False}
        path_info = self.adls_client.info(adls_path)
        if path_info['type'] == 'FILE':
            return {
                'fullPath' : path_lnt, 
                'exists' : True, 
                'directory' : False, 
                'size' : path_info['blockSize'], 
                'lastModified':path_info['modificationTime']
            }
        else:
            children = []
            for child in self.adls_client.listdir(adls_path):
                child_info = self.adls_client.info(child)
                child_sub_path = self.get_lnt_path(child)[len(self.root_lnt):]
                if child_info['type'] == 'DIRECTORY':
                    children.append({
                        'fullPath' : child_sub_path, 
                        'exists' : True, 
                        'directory' : True, 
                        'size' : 0
                    })
                else:
                    children.append({
                        'fullPath' : child_sub_path, 
                        'exists' : True, 
                        'directory' : False, 
                        'size' : child_info['blockSize'], 
                        'lastModified':child_info['modificationTime']
                    })
            return {
                'fullPath': path_lnt, 
                'exists' : True, 
                'directory' : True, 
                'children' : children
            }
        
    def enumerate(self, path, first_non_empty):
        adls_path = self.get_adls_lnt_path(path)
        path_lnt = self.get_lnt_path(path)
        if not self.adls_client.exists(adls_path):
            return None
        path_info = self.adls_client.info(adls_path)
        if path_info['type'] == 'FILE':
            return [{
                'path' : path_lnt.split("/")[-1], 
                'size' : path_info['blockSize'], 
                'lastModified' : path_info['modificationTime']
            }]
        else:
            paths = []
            for child in self.adls_client.walk(adls_path):
                child_info = self.adls_client.info(child)
                child_sub_path = self.get_lnt_path(child)[len(self.root_lnt):]
                if self.adls_client.info(child)['type'] == 'FILE':
                    paths.append({
                        'path' : child_sub_path, 
                        'size' : child_info['blockSize'], 
                        'lastModified' : child_info['modificationTime']})
            return paths        
        
    def stat(self, path):
        adls_path = self.get_adls_lnt_path(path)
        path_lnt = self.get_lnt_path(path)
        if not self.adls_client.exists(adls_path):
            return None
        path_info = self.adls_client.info(adls_path)
        if path_info['type'] == 'DIRECTORY':
            return {
                'path' : path, 
                'size' : 0, 
                'lastModified' : path_info['modificationTime'], 
                'isDirectory' : True
            }
        else:
            return {
                'path' : path, 
                'size' : path_info['blockSize'], 
                'lastModified' : path_info['modificationTime'], 
                'isDirectory': False
            }
        
    def read(self, path, stream, limit):
        adls_path = self.get_adls_lnt_path(path)
        if not self.adls_client.exists(adls_path):
            raise Exception('Path doesn t exist : %s' % adls_path)
        with self.adls_client.open(adls_path, 'rb') as f:
            shutil.copyfileobj(f, stream)

    def close(self):
        print "closed"