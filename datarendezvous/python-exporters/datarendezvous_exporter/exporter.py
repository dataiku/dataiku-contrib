# -*- coding: utf-8 -*-

from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper
from datarendezvous_api import DrdvApi
import os
import csv
from cStringIO import StringIO
import io
import time
import tempfile

CREATE_DATASET = 'create_dataset'
CREATE_VERSION = 'create_version'

class DrdvExporter(Exporter):

    def __init__(self, config, plugin_config):
        
        self.config = config
        self.plugin_config = plugin_config
        self.row_index = 0
        self.row_buffer = []
        self.row_buffer_size = 1000;
        
        # Form data
        self.api_key = self.config.get("api_key", None)
        self.dataset_id = self.config.get("dataset_id", None)
        self.datapot_id = self.config.get("datapot_id", None)
        self.export_type = self.config.get("export_type", None)
        self.dataset_name = self.config.get("dataset_name", 'Unnamed')
        
        self.api = DrdvApi(self.api_key)
        

    def open(self, schema):
        
        self.schema = schema


    def write_row(self, row):
        
        if self.row_index == 0:
            
            
            file = StringIO()
            first_row_with_headers = []
            first_row_with_headers.append([ column['name'].encode('utf-8') for column in self.schema['columns']])
            first_row_with_headers.append(row)
            file.write(u'\ufeff'.encode('utf8'))
            csv.writer(file).writerows(first_row_with_headers)
            file.flush()
            file.seek(0)
            file_key = self.api.upload(file)
            file.close()
            
            
            if self.export_type == CREATE_DATASET:
                self.new_dataset_id = self.api.new_dataset(self.datapot_id, self.dataset_name, file_key)
            
            elif self.export_type == CREATE_VERSION:
                self.new_dataset_id = self.api.new_dataset_version(self.datapot_id, self.dataset_id, file_key)
            else:
                raise Exception("Invalid export_type %s" % export_type)
                
            # /!\ To be fixed /!\
            time.sleep(20)
            
        else :
            row_obj = {}
            for (col, val) in zip(self.schema["columns"], row):
                row_obj[col["name"]] = val
            self.row_buffer.append(row_obj)
            
            if len(self.row_buffer) > self.row_buffer_size:
                self.api.dataset_insert(self.new_dataset_id, self.row_buffer)
                self.row_buffer = []
            
        self.row_index += 1
       
        
    def close(self):  
        
        if len(self.row_buffer) > 0:
            self.api.dataset_insert(self.new_dataset_id, self.row_buffer)
            self.row_buffer = []
