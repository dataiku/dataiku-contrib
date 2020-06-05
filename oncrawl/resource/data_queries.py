import requests
import prison
import pendulum
import oncrawl as oc
import json

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
        
        headers['Authorization'] = 'Bearer {}'.format(config['api_key'])
        
        filters = ()
        offset = 0
        limit = 1000
        sort = 'name:asc'
        
        if 'offset' in payload:
            offset = payload['offset']
            
        if 'limit' in payload and payload['limit'] is not None:
            limit = payload['limit']
            
        if 'sort' in payload:
            sort = payload['sort'].replace(" ", "")

        response = {
            'projects':{}, 
        }
         
        while offset is not None:

            try:
                
                get_projects = requests.get('https://app.oncrawl.com/api/v2/projects?filters={}&offset={}&limit={}&sort={}'.format(filters, offset, limit, sort), headers = headers)
                get_projects.raise_for_status()
                projects = get_projects.json()

                offset = projects['meta']['offset'] + projects['meta']['limit']

                for project in projects['projects']:
                    response['projects'][project['id']] = project['name']
                    
                assert offset <= projects['meta']['total']

            except AssertionError:
                offset = None
                
            except requests.exceptions.HTTPError: 
                offset = None
                response = {'error' : 'error : {}'.format(get_projects)}

            except Exception as e:
                offset = None
                response = {'error' : get_projects}
        
        return response
    
    #get crawls
    if payload["method"] == "get_crawls":

        headers['Authorization'] = 'Bearer {}'.format(config['api_key'])
        
        offset = 0
        limit = 1000
        
        try:
            assert payload['projects_id'] == 'all'
            projects_id = list(config['list_projects_id_name'].keys())
            
        except AssertionError as error:
            projects_id = [config['projects_id'].split(',')[0]]

        
        # work with date as string to support manual date override
        # do not forget that range requested is [[ => always add 1 day !!
        dates = oc.build_date_range(config)
        date_start_yyyy_mm_dd = dates['start']
        date_end_yyyy_mm_dd = dates['end']
          
        user_tz = pendulum.now().timezone.name
        
        crawl_start_timestamp = pendulum.parse(date_start_yyyy_mm_dd, tz=user_tz).timestamp() * 1000
        crawl_end_timestamp = pendulum.parse(date_end_yyyy_mm_dd, tz=user_tz).timestamp() * 1000
        
        filters = {
                "and" : [
                    {"field": ["status", "equals", "done"]},
                    {"field": ["created_at", "gte", crawl_start_timestamp]},
                    {"field": ["created_at", "lt", crawl_end_timestamp]},
                    {"field": ["project_id", "one_of", projects_id]}
                ]
            }

        response = {
            'configs':{}, 
            'crawls':{}, 
        }
        
        try:
            
            while offset is not None:
                
                get_crawls = requests.get('https://app.oncrawl.com/api/v2/crawls?filters={}&offset={}&limit={}&sort=created_at:desc'.format(prison.dumps(filters), offset, limit), headers = headers)
                get_crawls.raise_for_status()
                
                crawls = get_crawls.json()
                offset = crawls['meta']['offset'] + crawls['meta']['limit']
                
                for crawl in crawls['crawls']:

                    if ( config['index'] == 'pages' and crawl['status'] == 'done') or (config['index'] == 'links' and crawl['link_status'] == 'live'):

                        if crawl['crawl_config']['name'] not in response['configs']:
                            response['configs'][crawl['crawl_config']['name']] = []

                        response['configs'][crawl['crawl_config']['name']].append(crawl['id'])
                        response['crawls'][crawl['id']] = {
                            "project_id": crawl['project_id'],
                            "created_at": crawl['created_at'],
                            "ended_at": crawl['ended_at'],
                        }
                        
                assert offset <= crawls['meta']['total']
        
        except AssertionError:
                offset = None
                
        except requests.exceptions.HTTPError as e: 
            offset = None
            response = {'error' : 'merguez error : {} {}'.format(str(e), get_crawls.text)}
                
        except Exception as e:
            response = {'error' : get_crawls}
            
        return response
    