import json
import urlparse
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging

# Session object for requests
s = requests.Session()
# Retry strategy (cf http://stackoverflow.com/a/35504626/4969056)
retries = Retry(total=3,
                backoff_factor=2)
s.mount('https://', HTTPAdapter(max_retries=retries))

def make_api_call(action, token, parameters = {}, method = 'get', data = {}):
    """
    Makes an API call to Intercom
    """
    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip',
        'Authorization': 'Bearer %s' % token
    }
    if method == 'get':
        r = s.request(method, 'https://api.intercom.io/'+action, headers=headers, params=parameters, timeout=30)
    else:
        raise ValueError('Unimplemented method.')
    logging.info('Intercom plugin - API %s call: %s' % (method, r.url) )
    if r.status_code < 300:
        return r.json()
    else:
        raise Exception('API error when calling %s (code %i): %s' % (r.url, r.status_code, r.content))

def list_object(object_name, token, parameters = {}):
    """
    A generator that wraps make_api_call() to get all items of an object
    """
    looping = True
    parameters.update({'page': None})
    while looping:
        json = make_api_call(object_name, token, parameters)
        logging.info('Intercom plugin - %s: results = %i' % (object_name, len(json.get(object_name))) )
        for r in json.get(object_name):
            yield r
        next = json.get('pages', {}).get('next')
        logging.info('Intercom plugin - %s: next = %s' % (object_name, next) )
        if next is None:
            looping = False
        else:
            # next contains an url, let's extract the url params
            new_params = dict(urlparse.parse_qs(urlparse.urlparse(next).query))
            parameters.update(new_params)


def scroll_object(object_name, token, parameters = {}):
    """
    A generator that wraps make_api_call() to get all items of an object using the scpecial scroll endpoint
    """
    page = None
    looping = True
    while looping:
        parameters.update({'scroll_param':page})
        json = make_api_call(object_name+'/scroll', token, parameters)
        logging.info('Intercom plugin - %s: scroll_param = %s' % (object_name, json.get("scroll_param")) )
        logging.info('Intercom plugin - %s: results = %i' % (object_name, len(json.get(object_name))) )
        for r in json.get(object_name):
            yield r
        page = json.get('scroll_param')
        if page is None or len(json.get(object_name)) == 0:
            looping = False

def make_row_compatible_with_dss_schema(row):
    return {
        k: json.dumps(v) if not isinstance(v, str) and not isinstance(v, unicode) else v
        for k, v in row.iteritems()
    }
