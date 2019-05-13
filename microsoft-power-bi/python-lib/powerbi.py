import sys
import json 
import dataiku
import requests
import datetime


# Data types mapping DSS => Power BI
fieldSetterMap = {
    'boolean':  'Boolean',
    'tinyint':  'Int64',
    'smallint': 'Int64',
    'int':      'Int64',
    'bigint':   'Int64',
    'float':    'Double',
    'double':   'Double',
    'date':     'String',
    'string':   'String',
    'array':    'String',
    'map':      'String',
    'object':   'String'
}

# Main interactor object
class PowerBI(object):
    
    def __init__(self, token):
        self.token = token
        self.headers = {
            'Authorization': 'Bearer ' + self.token,
            'Content-Type': 'application/json'
        }
        
    def get_datasets(self):
        endpoint = 'https://api.powerbi.com/v1.0/myorg/datasets'
        response = requests.get(endpoint, headers=self.headers)
        return response
    
    def get_dataset_by_name(self, name):
        data = self.get_datasets()
        datasets = data.json().get('value')
        o = []
        if datasets:
            for dataset in datasets:
                if dataset['name'] == name:
                    o.append(dataset['id'])
        return o
    
    def delete_dataset(self, dsid):
        endpoint = 'https://api.powerbi.com/v1.0/myorg/datasets/{}'.format(dsid)
        response = requests.delete(endpoint, headers=self.headers)
        print "[+] Deleted existing Power BI dataset {} (response code: {})...".format(
            dsid, response.status_code
        )
        return response
    
    def create_dataset_from_dss(self, pbi_dataset=None, pbi_table="dataiku-data", dss_dataset=None, overwrite=True):
        o = {}
        ds = dataiku.Dataset(dss_dataset)
        o['pbi-dataset'] = pbi_dataset
        o['pbi-table']   = pbi_table
        o['dss-dataset'] = dss_dataset
        o['created-at']  = str(datetime.datetime.utcnow())
        # Delete existing dataset
        if overwrite:
            print "[+] Overwriting existing Power BI dataset..."
            datasets = self.get_dataset_by_name(pbi_dataset)
            for dataset in datasets:
                self.delete_dataset(dataset)            
        # Build the Power BI Dataset schema
        columns = []
        for column in ds.read_schema():
            c = {}
            c["name"] = column["name"]
            c["dataType"] = fieldSetterMap.get(column["type"], "String")
            columns.append(c)            
        # Power BI dataset definition
        payload = {
            "name": pbi_dataset,
            "defaultMode" : "PushStreaming",
            "tables": [
                {
                    "name": pbi_table,
                    "columns": columns
                }
            ]
        }
        response = requests.post(
            "https://api.powerbi.com/v1.0/myorg/datasets", 
            data=json.dumps(payload), 
            headers=self.headers
        )
        o['response-dataset'] = response.status_code
        o['dataset-id'] = response.json().get('id', None)
        if o['dataset-id'] is None:
            print "[-] ERROR while trying to retrieve a dataset id. Your token may need to be refreshed."
            sys.exit(1)
        else:
            print "[+] Created Power BI dataset {} (dataset: {}) from {}".format(
                o['dataset-id'],
                o['pbi-dataset'],
                o['dss-dataset']
            )
        return o
    
    def create_dataset_from_schema(self, pbi_dataset=None, pbi_table=None, schema=None):
        # Build the Power BI Dataset schema
        columns = []
        for column in schema["columns"]:
            c = {}
            c["name"] = column["name"]
            c["dataType"] = fieldSetterMap.get(column["type"], "String")
            columns.append(c)
        # Power BI dataset definition
        payload = {
            "name": pbi_dataset,
            "defaultMode" : "PushStreaming",
            "tables": [
                {
                    "name": pbi_table,
                    "columns": columns
                }
            ]
        }
        response = requests.post(
            "https://api.powerbi.com/v1.0/myorg/datasets", 
            data=json.dumps(payload), 
            headers=self.headers
        )
        return response.json()
    
    def load_data_from_dss(self, dataset, append=False):

        dsid = dataset['dataset-id']
        
        # Empty existing rows
        if not append:
            print "[+] Deleting existing records..."
            requests.delete(
                "https://api.powerbi.com/v1.0/myorg/datasets/{}/tables/{}/rows".format(dsid, dataset['pbi-table']),
                headers=self.headers
            )
        
        # Load records
        ds = dataiku.Dataset(dataset['dss-dataset'])
        _o = []
        i = 0
        for block in ds.iter_dataframes(chunksize=1000):
            i = i + 1
            rows = {}
            rows["rows"] = []
            for index, record in block.iterrows():
                rows["rows"].append(record.to_dict())
                
            response = requests.post(
                "https://api.powerbi.com/v1.0/myorg/datasets/{}/tables/{}/rows".format(dsid, dataset['pbi-table']),
                data=json.dumps(rows),
                headers=self.headers
            )
            _o.append(response.status_code)
            print "[+] Loaded {} records...".format(i*1000)
        dataset['responses-load'] = _o
        print "[+] Loading complete"
        dataset['loaded-at']  = str(datetime.datetime.utcnow())
            
        return dataset

    
def generate_access_token(username=None, password=None, client_id=None, client_secret=None):
    """
      Call the Azure API's to retrieve an access token to interact with Power BI.
      Requires full credentials to be passed.
    """
    data = {
        "username"     : username,
        "password"     : password,
        "client_id"    : client_id,
        "client_secret": client_secret,
        "resource"     : "https://analysis.windows.net/powerbi/api",
        "grant_type"   : "password",
        "scope"        : "openid"
    }
    response = requests.post('https://login.microsoftonline.com/common/oauth2/token', data=data)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print('HTTPError: ' + str(e) + ' response content:' + response.content)
        sys.exit("Error getting token")
        
    return response.json()
    
