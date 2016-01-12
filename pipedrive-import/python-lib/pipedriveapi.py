"""
This files contains kind of "wrapper functions" for Pipedrive API and an utility function to slugify columns names.
"""

import requests
import json
from slugify import slugify

list_unique_slugs = []

def make_api_call(conf, action, params = {}, method = 'get', data = {}):
    """
    Make an API call to Pipedrive.
    The method can be 'get' or 'post'.
    """
    parameters = {
        'api_token': conf['API_KEY'],
        'api_output': 'json'
    }
    parameters.update(params)
    headers = {
        'Content-type': 'application/json'
    }
    if method == 'get':
        r = requests.request(method, conf['API_BASE_URL']+action, params=parameters)
    elif method == 'post':
        r = requests.request(method, conf['API_BASE_URL']+action, data=data, params=parameters)
    else:
        raise ValueError('Method should be get or post.')
    print 'API call: ' + r.url
    if ((r.status_code == 200 and method == 'get') or (r.status_code == 201 and method == 'post')) and r.json().get('success') == True:
        return r.json()
    else:
        if 'error' in r.json():
            raise IOError('API error ("%s") when calling: %s' % (r.json().get('error'), r.url) )
        else:
            raise IOError('API error (unknown) when calling: %s' % r.url )
        

def make_api_call_all_pages(conf, action, params = {}):
    """
    A wrapper of make_api_call() to get all pages on a GET request
    """
    start = 0
    results = []
    looping = True
    params.update({'limit':conf['PAGINATION']})
    while looping:
        params.update({'start':start})
        json = make_api_call(conf, action, params)
        for r in json.get('data'):
            results.append(r)
        is_more = json.get('additional_data').get('pagination').get('more_items_in_collection')
        if is_more:
            start = json.get('additional_data').get('pagination').get('next_start')
        else:
            looping = False
    return results

def get_unique_slug(string):
    """
    Gives a unique slugified string from a string.
    """
    string = slugify(string, to_lower=True, max_length=25, separator="_", capitalize=True)
    if string == '':
        string = 'none'
    test_string = string
    i = 0
    while test_string in list_unique_slugs:
        i += 1
        test_string = string + '_' + str(i)
    list_unique_slugs.append(test_string)
    return test_string