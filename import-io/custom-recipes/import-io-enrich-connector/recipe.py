import urllib
import json
from dataiku.customrecipe import *
import importio_utils

parameters_map = json.loads(get_recipe_config()["parameters_map"])

def build_query(in_row, apikey):
    input_params = [
        "input="+importio_param+":" + urllib.quote(safe='', s=in_row[col])
        for importio_param, col in parameters_map.items() ]
    return "&".join(input_params) + '&_apikey=' + apikey

importio_utils.run(build_query)
