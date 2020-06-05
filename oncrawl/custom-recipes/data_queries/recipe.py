# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *
import requests
import json
from oncrawl import oncrawlDataAPI as ocd

output_names = get_output_names_for_role('output')
output_datasets = [dataiku.Dataset(name) for name in output_names]
output = output_datasets[0]

#------------------------------config & vars
config = get_recipe_config()

headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization' : 'Bearer {}'.format(config['api_key'])
}

#--according index, ids are related to projects or crawls - each id represent a crawl or a project
if config['index'] != 'logs':
    ids = config['list_configs_crawls'][config['crawl_config']]
    if config['crawls_id'] != 'all':
        ids = [config['crawls_id']]
else:    
    ids = config['list_projects_id_name'].keys()
    if config['projects_id'] != 'all':
        ids = [config['projects_id'].split(',')[0]]

#------------------------------schema
#fields not returned by oncrawl API
metadata = {
    'project_id': 'string',
    'project_name': 'string',
    'crawl_id': 'string',
    'config_name': 'string',
    'crawl_start_timestamp': 'bigint'
    }

metadata_fields = ocd.build_schema_from_metadata(config, metadata)

schema = {
    'dataset_schema': metadata_fields['schema'],
    'dataset_schema_field_list': metadata_fields['list']
}
fields_to_request_by_ids = {}

for i, id in enumerate(ids):
    
    progress = '#{} {} {}/{}'.format(id, 'crawls' if config['index'] != 'logs' else 'projects', (i+1), len(ids))
    
    #when aggregate data, all items have same schema
    if config['data_action'] == 'aggs':
        
        if i == 0:
            f = ocd.build_schema_from_config(config=config)
            schema['dataset_schema'] = schema['dataset_schema'] + f
            print('############################\r\n############################\r\nBuil dataset schema with: ', progress)
        else:
            break        
    else:
        
        print('############################\r\n############################\r\nBuil dataset schema with: ', progress)
        #when export data, for many reasons all items could not have same schema        
        #return new fields to add to dataset schema and all fields to request for this item
        f = ocd.build_schema_from_oncrawl(config=config, id=id, headers=headers, schema=schema)
        
        if 'item_schema' not in f.keys() or len(f['item_schema']) == 0:
            continue
        
        schema['dataset_schema'] = schema['dataset_schema'] + f['dataset_schema']
        schema['dataset_schema_field_list'] = schema['dataset_schema_field_list'] + f['dataset_schema_field_list']

        fields_to_request_by_ids[id] = f['item_schema']
    
output.write_schema(schema['dataset_schema'])

#------------------------------data & writer
total_count = 0
with output.get_writer() as writer:
        
    for i, id in enumerate(ids):

        #this case happend when project has no log feature or unexpected ES issue
        if config['data_action'] == 'export' and id not in fields_to_request_by_ids.keys():
            continue
        
        progress = '#{} {} {}/{}'.format(id, 'crawls' if config['index'] != 'logs' else 'projects', (i+1), len(ids))
        print('############################\r\n############################\r\nGet data for: ', progress)
    
        metadata_value = ocd.fill_metadata(config, id)
        if config['data_action'] == 'export':
            data = ocd.export(config_oql=config['oql'], fields=fields_to_request_by_ids[id], config_index=config['index'], id=id, headers=headers)
        else:
            data = ocd.aggs(config_oql=config['oql'], config_index=config['index'], id=id, headers=headers)

        count_result = 0
        try:
            for json_line in data:
                
                row = metadata_value + []
                
                if config['data_action'] == 'export':
                    #oncrawl export api send values not in the same order as schema...
                    for field in schema['dataset_schema_field_list']:
                        if field not in list(metadata.keys()):
                            if field in list(json_line.keys()):
                                if field in ['title', 'meta_description', 'h1'] and json_line[field] is not None:
                                    row.append(json_line[field].encode(encoding = 'utf8', errors = 'replace'))
                                else:
                                    row.append(json_line[field])
                            else:
                                row.append(None)
                else:
                    row = row + list(json_line.values())

                writer.write_row_array(row)
                count_result += 1
                print(progress, 'row: ',count_result)
            print(progress, ': total row recorded: ', count_result, '\r\n############################\r\n############################')
   
        except Exception as e:
            raise Exception('{}'.format(e))
            
        total_count += 1

