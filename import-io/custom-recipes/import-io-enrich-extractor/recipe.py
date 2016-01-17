import dataiku, urllib, urlparse, requests, sys
from dataiku.customrecipe import *
import importio_utils

url_field = get_recipe_config()['column_containing_url']

def build_query(in_row, apikey):
    url = in_row[url_field]
    return 'input=webpage/url:' + urllib.quote(safe='',s=url) + '&_apikey=' + apikey
          # input/webpage/url=

importio_utils.run(build_query)
