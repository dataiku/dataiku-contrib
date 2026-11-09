import requests
import json

endpoint_by_index = {
    'pages': {'end_point' : 'crawl/__id__/pages'},
    'links': {'end_point' : 'crawl/__id__/links'},
    'logs': {'end_point' : 'project/__id__/log_monitoring/events'}
}

def map_dss_storage_type(field_type):
    
    mapping = {
        'bool':'boolean',
        'float':'double',
        'ratio':'double',
        'object':'object'
    }
    
    dss_type = mapping.get(field_type, 'string')
    
    return dss_type

def fill_metadata(config, id):
    
    p_id = id
    if config['index'] != 'logs':
        p_id = config['list_crawls_project'][id]['project_id']
        
    v = [p_id, config['list_projects_id_name'][p_id]]
    if config['index'] != 'logs':
        v = v + [id, config['crawl_config'], config['list_crawls_project'][id]['created_at']]

    return v


def build_schema_from_metadata(config, metadata):
    
    fields = list(metadata.keys())
    if config['index'] == 'logs':
        fields = list(metadata.keys())[:2]
        
    f = {
        'list': fields,
        'schema': [{'name': field, 'type': metadata[field]} for field in fields]
    }
    
    return f
    
def build_schema_from_oncrawl(config, id, headers, schema):
   
    f = {'dataset_schema': [], 'dataset_schema_field_list': [], 'item_schema': []}
    
    fields = get_fields(config_index=config['index'], id=id, headers=headers)
    
    try:     
        for field in fields['fields']:

            if field['can_display']:

                #item field list
                f['item_schema'].append(field['name'])

                #look for new fields to add to dataset schema
                if field['name'] not in schema['dataset_schema_field_list']:
                    field_type = map_dss_storage_type(field['type'])

                    f['dataset_schema'].append({
                      "name": field['name'],
                      "type": field_type,
                    })

                    f['dataset_schema_field_list'].append(field['name'])
    
    except Exception as e:
        print('############################\r\nProject {} has no logs monitoring feature\n\r############################'.format(id))
            
    return f

def build_schema_from_config(config):
    
    f = []
    oql = json.loads(config['oql'])
    
    for i, agg in enumerate(oql['aggs']):

        field_type = 'bigint'
        if 'fields' in agg.keys():
            field_type = 'object'
            
        f.append({
              "name":agg['name'],
              "type": field_type,
            })
   
    return f

def get_fields(id, config_index, headers):
    
    endpoint = endpoint_by_index[config_index]['end_point'].replace('__id__', id)
    
    try:
            
        #get fields = dataset cols
        get_fields = requests.request('GET', 'https://app.oncrawl.com/api/v2/data/{}/fields'.format(endpoint), headers=headers)
        get_fields.raise_for_status()

        fields = get_fields.json()
        
        return fields

    except requests.exceptions.HTTPError as e: 
        if config_index != 'logs' and get_fields.status_code != 403:
            raise Exception('{}-{}'.format(str(e), get_fields.text))
        else:
            return

    except Exception as e:
            raise Exception(e)
    
    
def export(config_oql, fields, config_index, id, headers):
        
        endpoint = endpoint_by_index[config_index]['end_point'].replace('__id__', id)
 
        #oql = oncrawl query language - interface to query our ES
        oql = json.loads(config_oql)['oql']
        body = {
                
            'oql' : oql,
            'fields' : fields,
            'file_type':'json'
        }
        
        #get urls = dataset rows
        try:
            export = requests.request('POST', 'https://app.oncrawl.com/api/v2/data/{}?export=true'.format(endpoint), json=body, headers=headers, stream=True)
            export.raise_for_status()
            
            for line in export.iter_lines():
                json_line = json.loads(line)
                
                yield json_line
                
        except requests.exceptions.HTTPError as e: 
            if config_index != 'logs' and export.status_code != 403:
                raise Exception('{}-{}'.format(str(e), export.text))
            
        except Exception as e:
            raise Exception(e)
                
        
def aggs(config_oql, config_index, id, headers):

    endpoint = endpoint_by_index[config_index]['end_point'].replace('__id__', id)
        
    oql = json.loads(config_oql)['aggs']

    body = {

        'aggs' : oql
    }

    try:

        get_data = requests.request('POST', 'https://app.oncrawl.com/api/v2/data/{}/aggs?fmt=row_objects'.format(endpoint), json=body, headers=headers)
        get_data.raise_for_status()
    
        data = get_data.json()
        
        agg_value = {}
        for j, agg in enumerate(data['aggs']):
            cols = agg['cols']
            col_name = cols[-1]
            agg_name = oql[j].get('name') if oql[j].get('name') else col_name

            if agg_name and agg_name in agg_value:
                agg_name = '{}_{}'.format(agg_name,j)

            if len(agg['rows']) == 1:
                agg_value[agg_name] = agg['rows'][0][col_name]
            else:
                agg_value[agg_name] = agg['rows']

        json_line = agg_value

        yield json_line

    except requests.exceptions.HTTPError as e: 
        if config_index != 'logs' and get_data.status_code != 403:
            raise Exception('{}-{}'.format(str(e), get_data.text))

    except Exception as e:
        error = e
                
        if data.get('aggs')[0].get('error'):
            error = data.get('aggs')[0].get('error')

        if config_index != 'logs':
            raise Exception(error)
        
        
        