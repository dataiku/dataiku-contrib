"""
This files contains kind of "wrapper functions" for Pipedrive API and an utility function to slugify columns names.
"""

import requests
import json
import re
import unicodedata
from unidecode import unidecode

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

def get_unique_slug(string):
    """
    Gives a unique slugified string from a string.
    """
    string = slugify(string, lower=True, space_replacement="_", only_ascii=True)
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

# Adapted from https://github.com/mozilla/unicode-slugify/blob/master/slugify/__init__.py
SLUG_OK = '-_'
def slugify(s, ok=SLUG_OK, lower=True, spaces=False, only_ascii=False, space_replacement='_'):
    """
    Creates a unicode slug for given string with several options.
    L and N signify letter/number.
    http://www.unicode.org/reports/tr44/tr44-4.html#GC_Values_Table
    :param s: Your unicode string.
    :param ok: Extra characters outside of alphanumerics to be allowed.
               Default is '-_~'
    :param lower: Lower the output string. 
                  Default is True
    :param spaces: True allows spaces, False replaces a space with the "space_replacement" param
    :param only_ascii: True to replace non-ASCII unicode characters with 
                       their ASCII representations.
    :param space_replacement: Char used to replace spaces if "spaces" is False. 
                              Default is dash ("-") or first char in ok if dash not allowed
    :type s: String
    :type ok: String
    :type lower: Bool
    :type spaces: Bool
    :type only_ascii: Bool
    :type space_replacement: String
    :return: Slugified unicode string
    """

    if only_ascii and ok != SLUG_OK and hasattr(ok, 'decode'):
        try:
            ok.decode('ascii')
        except UnicodeEncodeError:
            raise ValueError(('You can not use "only_ascii=True" with '
                              'a non ascii available chars in "ok" ("%s" given)') % ok)

    rv = []

    if not isinstance(s, unicode):
        # should handle that, but normally ok with Pipedrive API
        raise ValueError("The slugify function requires a unicode string.")

    for c in unicodedata.normalize('NFKC', s):
        cat = unicodedata.category(c)[0]
        if cat in 'LN' or c in ok:
            rv.append(c)
        elif cat == 'Z':  # space
            rv.append(' ')
    new = ''.join(rv).strip()

    if only_ascii:
        new = unidecode(new)
    if not spaces:
        if space_replacement and space_replacement not in ok:
            space_replacement = ok[0] if ok else ''
        new = re.sub('[%s\s]+' % space_replacement, space_replacement, new)
    if lower:
        new = new.lower()

    return new