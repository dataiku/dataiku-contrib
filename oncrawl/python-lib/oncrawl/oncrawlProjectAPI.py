import requests
import prison

headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
}

def build_human_error(response):
    
    r = 'Please apologize, something bad happened'
    
    if response.status_code == 401:
        r = 'Your API key seems to be invalid. Please check it and contact us if the error persists.'

    return r

def get_projects(api_key):
    
    headers['Authorization'] = 'Bearer {}'.format(api_key)
    
    offset = 0
    limit = 1000
    sort = 'name:asc'
    
    response = []
    
    while offset is not None:

        try:

            r = requests.get('https://app.oncrawl.com/api/v2/projects?&offset={}&limit={}&sort={}'.format(offset, limit, sort), headers = headers)
            r.raise_for_status()
            items = r.json()

            offset = items['meta']['offset'] + items['meta']['limit']

            for item in items['projects']:
                response.append({'id': item['id'], 'name':item['name']})

            assert offset <= items['meta']['total']

        except AssertionError:
            offset = None

        except requests.exceptions.HTTPError: 
            offset = None
            response = {'error': build_human_error(r)}
            
        except Exception as e:
            offset = None
            response = {'error' : e}
    
    return response

def get_live_crawls(config, projects_id, timestamp_range, limit=None):
    
    headers['Authorization'] = 'Bearer {}'.format(config['api_key'])

    offset = 0
    
    if limit is None:
        limit = 1000

    filters = {
        "and" : [
            {"field": ["status", "equals", "done"]},
            {"field": ["created_at", "gte", timestamp_range['start']]},
            {"field": ["created_at", "lt", timestamp_range['end']]},
            {"field": ["project_id", "one_of", projects_id]}
        ]
    }
    
    try:
            
        response = []
        while offset is not None:

            r = requests.get('https://app.oncrawl.com/api/v2/crawls?filters={}&offset={}&limit={}&sort=created_at:desc'.format(prison.dumps(filters), offset, limit), headers = headers)
            r.raise_for_status()

            items = r.json()
            offset = items['meta']['offset'] + items['meta']['limit']

            for item in items['crawls']:

                if ( config['index'] == 'pages' and item['status'] == 'done') or (config['index'] == 'links' and item['link_status'] == 'live'):
                    
                    response.append(
                        {
                            'id': item['id'],
                            'config_name': item['crawl_config']['name'],
                            'project_id': item['project_id'],
                            'created_at': item['created_at'],
                            'ended_at': item['ended_at']
                        }
                   )

            assert offset <= items['meta']['total']

    except AssertionError:
            offset = None

    except requests.exceptions.HTTPError as e: 
        offset = None
        response = {'error': build_human_error(r)}

    except Exception as e:
        response = {'error' : e}

    return response

