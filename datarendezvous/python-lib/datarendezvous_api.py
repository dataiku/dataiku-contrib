# -*- coding: utf-8 -*-

import requests
import time

class DrdvApi():
    
    BASE_URI = 'https://api.datarendezvous.com/api/v1'
    UPLOAD_ENDPOINT = BASE_URI + '/upload'
    DATASET_ENDPOINT = BASE_URI + '/datasets'
    DATASET_VERSION_ENDPOINT = BASE_URI + '/datasets/%(dataset_id)s/version'
    DATASET_INSERT_ENDPOINT = BASE_URI + '/datasets/%(dataset_id)s/data'
    DATASET_EXPORT_ENDPOINT = BASE_URI + '/datasets/%(dataset_id)s/export'
    DATASET_STATUS_ENDPOINT = BASE_URI + '/datasets/%(dataset_id)s/status'
    UPLOAD_FILE_NAME = "dataiku.csv"
    
    MAX_RETRY = 15
    
    
    def __init__(self, api_key):
        self.api_key = api_key
        self.session = requests.Session()
        
    def _headers(self):
        return {'Authorization': self.api_key, 'Accept': 'application/json' }
        
    def upload(self, file):
        files = {'file': ( self.UPLOAD_FILE_NAME, file, 'text/csv' ) }
        response = self.session.post(self.UPLOAD_ENDPOINT, files=files, headers=self._headers())
        return response.json()['file_key']
    
    def export(self, dataset_id, limit = None):
        params = {}
            
        if limit and limit > 0:
            params['limit'] = limit
        elif limit and limit == -1:
            params['limit'] = 1000000
            
        response = self.session.get( self.DATASET_EXPORT_ENDPOINT % {"dataset_id": dataset_id} , headers = self._headers(), params = params)
        return response.json()['data']
    
    def new_dataset(self, datapot_id, name, file_key):

        payload = {}
        payload['name'] = name
        payload['datapot_id'] = datapot_id
        payload['file_key'] = file_key
        response = self.session.put(self.DATASET_ENDPOINT, json=payload, headers=self._headers())
        return self._waitFor(response.json()['dataset_id'])
    
    def new_dataset_version(self, datapot_id, dataset_id, file_key):
        
        payload = {}
        payload['name'] = name
        payload['datapot_id'] = datapot_id
        payload['file_key'] = file_key
        response = self.session.put(self.DATASET_VERSION_ENDPOINT % {"dataset_id": dataset_id}, json=payload, headers=self._headers())
        return self._waitFor(response.json()['dataset_id'])
    
    def dataset_insert(self, dataset_id, records):
        payload = {}
        payload['records'] = records
        response = self.session.post(self.DATASET_INSERT_ENDPOINT % {"dataset_id": dataset_id}, json=payload, headers=self._headers())
        response.raise_for_status()
        
    def dataset_status(self, dataset_id):
        response = self.session.get( self.DATASET_STATUS_ENDPOINT % {"dataset_id": dataset_id} , headers = self._headers())
        return response.json()['status']
    
    def _waitFor(self, dataset_id):
        retry = 0
        status = self.dataset_status(dataset_id)
        while status == 'pending' and retry < self.MAX_RETRY:
            time.sleep(1)
            status = self.dataset_status(dataset_id)
            retry += 1
        if status == 'error' or retry == self.MAX_RETRY:
            raise Exception('An error occured while initializing dataset %s' % dataset_id)
        return dataset_id
        
        
    
