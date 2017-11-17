"""
This files contains kind of a "wrapper function" for Airtable API and utility functions.
"""

import json
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Basic logging
def log(*args):
    for thing in args:
        if type(thing) is dict:
            thing = json.dumps(thing)
        print('Airtable plugin - %s' % thing)


# Session object for requests
s = requests.Session()
# Retry strategy (cf http://stackoverflow.com/a/35504626/4969056)
retries = Retry(total=3,
                backoff_factor=2)
s.mount('https://', HTTPAdapter(max_retries=retries))



def airtable_api(base, table, token, action = '', parameters = {}, method = 'get', data = {}):
    """
    Helper function to make calls to Airtable REST API.
    """

    headers = {
        'Content-type': 'application/json',
        'Accept-Encoding': 'gzip',
        'Authorization': 'Bearer %s' % token
    }
    url = "https://api.airtable.com/v0/%s/%s/%s" % (base, table, action)
    if method == 'get':
        r = s.request(method, url, headers=headers, params=parameters, timeout=10)
    elif method == 'post':
        r = s.request(method, url, headers=headers, data=json.dumps(data), params=parameters, timeout=10)
    else:
        raise Exception('Method should be get or post.')
    log('API %s call: %s' % (method, r.url) )
    if r.status_code < 300:
        return r.json()
    else:
        raise Exception('API error (%i) : %s' % (r.status_code, r.content))


