"""
This files contains kind of "wrapper functions" for Pipedrive API and an utility function to slugify columns names.
"""

import requests
import json
import re
import unicodedata

try:
    from unidecode import unidecode
    unidecode_available = True
except ImportError:
    unidecode_available = False

list_unique_slugs = []

date_reg = re.compile('^\d{4}-\d{2}-\d{2}$')
datetime_reg = re.compile('^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})$')

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

def slugify(s, lower=True):
    """
    Creates a slug (ascii) for a given unicode string.
    If the unidecode package is available, an ascii transliteration is done.
    """

    normalized =  unicodedata.normalize("NFD", s)
    cleaned = ''.join([c for c in normalized if unicodedata.category(c) != 'Mn'])
    slugified_ascii =  re.sub(r"[^A-Za-z0-9_-]", '_', cleaned)

    if unidecode_available:
        slugified_ascii = re.sub(r"[^A-Za-z0-9_-]", '_', unidecode(cleaned))

    slugified_ascii = re.sub(r"_{2,}", '_', slugified_ascii)

    if lower:
        slugified_ascii = slugified_ascii.lower()

    ### If you prefer to work with a unicode slug, use instead the following:
    # slugified_unicode = u""
    # for c in cleaned:
    #   cat = unicodedata.category(c)
    #   if cat.startswith("L") or cat.startswith("N"):
    #       slugified_unicode += c
    #   else:
    #       slugified_unicode += "_"

    return slugified_ascii

def get_unique_slug(string):
    """
    Gives a unique slugified string from a string.
    """
    string = slugify(string)
    if string == '':
        string = 'none'
    test_string = string
    i = 1
    while test_string in list_unique_slugs:
        i += 1
        test_string = string + '_' + str(i)
    list_unique_slugs.append(test_string)
    return test_string

def parse_date(string):
    """
    Parses a date/datetime given by Pipedrive and returns a ISO 8601 datetime.
    """

    if date_reg.match(string):
        return "%sT00:00:00Z" % string

    m = datetime_reg.match(string)
    if m:
        return "%sT%sZ" % (m.group(1), m.group(2))
    
    print "Not able to parse date: %s" % string
    return string
