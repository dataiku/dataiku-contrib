import oncrawl as oc
from oncrawl import oncrawlProjectAPI as ocp

def do(payload, config):
        
    headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        }
    
    if 'method' not in payload:
        return {}
    
    
    #handle date range
    if payload['method'] == 'build_date_range':

        dates = oc.build_date_range(config=config)
        
        date_start_yyyy_mm_dd = dates['start']
        date_end_yyyy_mm_dd = dates['end']
        
        return {'start': date_start_yyyy_mm_dd, 'end': date_end_yyyy_mm_dd}
        
        
    #get projects
    if payload["method"] == "get_projects":
        
        response = {
            'projects':{}, 
        }
        
        try:
            
            projects = ocp.get_projects(config['api_key'])
            
            for p in projects:
                response['projects'][p['id']] = p['name']
            
        except Exception as e:
            response = {'error' : projects['error']}
       
        return response
    
    
    #get crawls
    if payload["method"] == "get_crawls":
        
        #project list
        try:
            assert payload['projects_id'] == 'all'
            projects_id = list(config['list_projects_id_name'].keys())
            
        except AssertionError as error:
            projects_id = [config['projects_id'].split(',')[0]]
            
            
        #dates ranges: need timestamp
        # work with date as string to support manual date override
        dates = oc.build_date_range(config)
        date_start_yyyy_mm_dd = dates['start']
        date_end_yyyy_mm_dd = dates['end']
          
        crawl_start_timestamp = oc.datestring_to_miltimestamp_with_tz(dates['start'])
        crawl_end_timestamp = oc.datestring_to_miltimestamp_with_tz(dates['end'])
            
        crawls = ocp.get_live_crawls(projects_id=projects_id, config=config, timestamp_range={'start': crawl_start_timestamp, 'end': crawl_end_timestamp})
        
        response = {
            'configs':{}, 
            'crawls':{}, 
        }
        
        try:
            
            for c in crawls:
                
                if c['config_name'] not in response['configs']:
                    response['configs'][c['config_name']] = []

                response['configs'][c['config_name']].append(c['id'])
                response['crawls'][c['id']] = {
                    "project_id": c['project_id'],
                    "created_at": c['created_at'],
                    "ended_at": c['ended_at'],
                }

        except Exception as e:
            response = {'error' : crawls['error']}

            
        return response
    
    
    