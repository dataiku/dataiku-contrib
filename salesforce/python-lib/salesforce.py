"""
This files contains kind of "wrapper functions" for Salesforce API and utility functions.
"""

import json
import datetime
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Basinc logging
def log(*args):
    for thing in args:
        if type(thing) is dict:
            thing = json.dumps(thing)
        print('Salesforce plugin - %s' % thing)

# Session object for requests
s = requests.Session()
# Retry strategy (cf http://stackoverflow.com/a/35504626/4969056)
retries = Retry(total=3,
                backoff_factor=2)
s.mount('https://', HTTPAdapter(max_retries=retries))

# Global variables
API_BASE_URL = ''
ACCESS_TOKEN = ''

def make_api_call(action, parameters = {}, method = 'get', data = {}):
    """
    Makes an API call to SalesForce
    Parameters: action (the URL), params, method (GET or POST), data for POST
    """
    headers = {
        'Content-type': 'application/json',
        'Accept-Encoding': 'gzip',
        'Authorization': 'Bearer %s' % ACCESS_TOKEN
    }
    if method == 'get':
        r = s.request(method, API_BASE_URL+action, headers=headers, params=parameters, timeout=30)
    elif method == 'post':
        r = s.request(method, API_BASE_URL+action, headers=headers, data=data, params=parameters, timeout=10)
    else:
        raise ValueError('Method should be get or post.')
    log('API %s call: %s' % (method, r.url) )
    if ((r.status_code == 200 and method == 'get') or (r.status_code == 201 and method == 'post')):
        return r.json()
    else:
        raise ValueError('API error when calling %s : %s' % (r.url, r.content))


def iterate_dict(d, parents=[]):
    """
    This function iterates over one dict and returns a list of tuples: (list_of_keys, value)
    Usefull for looping through a multidimensional dictionary.
    """

    r = []
    for k,v in d.iteritems():
        if isinstance(v, dict):
            r.extend(iterate_dict(v, parents + [k]))
        elif isinstance(v, list):
            r.append((parents + [k], v))
        else:
            r.append((parents + [k], v))
    return r

def transform_json_to_dss_columns(row_obj):
    """
    Iterates over a JSON to tranfsorm each element into a column.
    Example:
    {'a': {'b': 'c'}} -> {'a.b': 'c'}
    """
    row = {}
    for keys, value in iterate_dict(row_obj):
        row[".".join(keys)] = value
    return row

