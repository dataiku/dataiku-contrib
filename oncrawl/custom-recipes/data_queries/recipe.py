# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import get_output_names_for_role, get_recipe_config
import oncrawl as oc
from oncrawl import oncrawlDataAPI as ocd
from oncrawl import oncrawlProjectAPI as ocp

output_names = get_output_names_for_role('output')
output_datasets = [dataiku.Dataset(name) for name in output_names]
output = output_datasets[0]

#------------------------------config & vars
config = get_recipe_config()

#config checker to raise better error
e = None
if 'api_key' not in config.keys():
    e = 'Please add your API key'
    
if 'list_projects_id_name' not in config.keys() or len(config['list_projects_id_name'].keys()) == 0:
    e = 'Your Oncrawl account seems to have no projects available. Please check with your Oncrawl account.'
    
if 'list_configs_crawls' not in config.keys() or len(config['list_configs_crawls'].keys()) == 0 or 'list_crawls_project' not in config.keys() or len(config['list_crawls_project'].keys()) == 0:
    e = 'Your Oncrawl account seems to have no crawls available. Please check the choosen project and date range with your Oncrawl account.' 
    
if e is not None:    
    raise Exception(e)    

headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization' : 'Bearer {}'.format(config['api_key'])
}

#list project ids
p_ids = []

#if getting all projects : rebuild an up to date ids list
if config['projects_id'] == 'all':
    try:
        p_ids_uptodate = ocp.get_projects(config['api_key'])
        
        for p in p_ids_uptodate:
            config['list_projects_id_name'][p['id']] = p['name']
            p_ids.append(p['id'])
            
    except Exception as e:
        raise Exception(p_ids_uptodate)
else:
    p_ids = [config['projects_id'].split(',')[0]]
    
#--list ids to get data : according config['index'], ids are related to projects or crawls - each id represents a crawl when index = pages or links and a project when index = logs
if config['index'] == 'logs':
    
    ids = p_ids
      
else:
    
    if config['crawls_id'] not in  ['all', 'last']:
        ids = [config['crawls_id']]
        
    else:
        
        #if getting all or last crawls : rebuild an up to date ids list
        try:
            dates = oc.build_date_range(config)
            date_start_yyyy_mm_dd = dates['start']
            date_end_yyyy_mm_dd = dates['end']

            crawl_start_timestamp = oc.datestring_to_miltimestamp_with_tz(dates['start'])
            crawl_end_timestamp = oc.datestring_to_miltimestamp_with_tz(dates['end'])

            limit = None
                
            c_ids_uptodate = ocp.get_live_crawls(projects_id=p_ids, config=config, timestamp_range={'start': crawl_start_timestamp, 'end': crawl_end_timestamp}, limit=limit)

            ids = []
            count_crawls_by_projects = []
            for c in c_ids_uptodate:
                if c['config_name'] == config['crawl_config']:
                    if (config['crawls_id'] == 'last' and c['project_id'] not in count_crawls_by_projects) or config['crawls_id'] != 'last':
                        count_crawls_by_projects.append(c['project_id'])
                        ids.append(c['id'])

        except Exception as e:
            raise
            
    
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

