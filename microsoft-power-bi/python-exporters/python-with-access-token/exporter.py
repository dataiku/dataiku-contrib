# This file is the actual code for the custom Python exporter python
import os
import sys
import json
import requests
from powerbi import *
from dataiku.exporter import Exporter
from dataiku.exporter import SchemaHelper
from math import isnan


class PowerBIExporter(Exporter):
    
    def __init__(self, config, plugin_config):
        self.config = config
        self.plugin_config = plugin_config
        self.row_index = 0
        self.row_buffer = {}
        self.row_buffer["rows"] = []
        # Plugin settings
        self.pbi_dataset     = self.config.get("dataset",       None)
        #self.pbi_table       = self.config.get("table",         None)
        self.pbi_table       = "dss-data"
        self.pbi_overwrite   = self.config.get("overwrite",     None)
        self.pbi_buffer_size = self.config.get("buffer_size",   None)
        self.dss_project_key = self.config.get("project_key",   None)
        # Retrieve access token from Project Variables
        with open(
            os.path.join(
                os.environ["DIP_HOME"], 
                "config", 
                "projects", 
                self.dss_project_key, 
                "variables.json"
            ), "r"
        ) as i:
            try:
                token = json.loads(i.read())["powerbi-settings"]["access_token"]
                # Interacting with Power BI API's
                self.headers = {
                    'Authorization': 'Bearer ' + token,
                    'Content-Type': 'application/json'
                }
                self.pbi = PowerBI(token)
            except Exception, e:
                print "ERROR [-] Error while reading your Power BI access token from Project Variables"
                print str(e)
                sys.exit("Authentication error")        

    def open(self, schema):
        self.schema = schema
        if self.pbi_overwrite:
            print "[+] Looking for Power BI datasets with similar names..."
            datasets = self.pbi.get_dataset_by_name(self.pbi_dataset)
            for dataset in datasets:
                self.pbi.delete_dataset(dataset)
            response = self.pbi.create_dataset_from_schema(
                pbi_dataset=self.pbi_dataset, 
                pbi_table=self.pbi_table, 
                schema=schema
            )
            if response.get("id") is None:
                print "ERROR [-] Error while creating your Power BI dataset."
                print "ERROR [-] Azure response:"
                print json.dumps(response, indent=4)
                sys.exit("Dataset creation error")
            self.dsid = response["id"]
            print "[+] Created Power BI dataset ID {}".format(self.dsid)
        else:
            datasets = self.pbi.get_dataset_by_name(self.pbi_dataset)
            if len(datasets) > 0:
                print "[+] Existing datasets: {}".format(datasets)
                self.dsid = datasets[0]
                print "[+] Will use Power BI dataset ID {}".format(self.dsid)
            else:
                print "ERROR [-] No existing dataset with name {}".format(self.pbi_dataset)
                print "ERROR [-] Check 'Overwrite' to create a new one"
                sys.exit("Dataset creation error")
                
            
    def write_row(self, row):
        row_obj = {}
        for (col, val) in zip(self.schema["columns"], row):
            if col['type'] in ['int', 'bigint', 'tinyint', 'smallint']:
                row_obj[col["name"]] = int(val) if val is not None and not isnan(val) else None
            else:
                row_obj[col["name"]] = val
        self.row_buffer["rows"].append(row_obj)
        if len(self.row_buffer["rows"]) > self.pbi_buffer_size:
            response = requests.post(
                "https://api.powerbi.com/v1.0/myorg/datasets/{}/tables/{}/rows".format(
                    self.dsid, 
                    self.pbi_table
                ),
                data=json.dumps(self.row_buffer["rows"]),
                headers=self.headers
            )
            print "[+] Inserted {} records (response code: {})".format(
                len(self.row_buffer["rows"]), 
                response.status_code
            )
            if not str(response.status_code).startswith('2'):
                print "[-] Response code {} may indicate an issue while loading your records.".format(response.status_code)
                print "[-] API response: {}".format(response.json())
            self.row_buffer["rows"] = []
        self.row_index += 1
        
    def close(self):
        if len(self.row_buffer["rows"]) > 0:
            response = requests.post(
                "https://api.powerbi.com/v1.0/myorg/datasets/{}/tables/{}/rows".format(
                    self.dsid, 
                    self.pbi_table
                ),
                data=json.dumps(self.row_buffer["rows"]),
                headers=self.headers
            )
            print "[+] Inserted {} records (response code: {})".format(
                len(self.row_buffer["rows"]), 
                response.status_code
            )
            if not str(response.status_code).startswith('2'):
                print "[-] Response code {} may indicate an issue while loading your records.".format(response.status_code)
                print "[-] API response: {}".format(response.json())
        print "[+] Loading complete."
        msg = ""
        msg = msg + "[+] {}".format("="*80) + "\n"
        msg = msg + "[+] Your Power BI dataset should be available at:" + "\n"
        msg = msg + "[+] https://app.powerbi.com/groups/me/datasets/{}".format(self.dsid) + "\n"
        msg = msg + "[+] {}".format("="*80)
        print msg

