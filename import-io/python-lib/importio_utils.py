# coding: UTF8
import urlparse
import requests
import sys
import dataiku
import time
from dataiku.customrecipe import *
import logging

logger = logging.getLogger(__name__)

# See http://api.docs.import.io/#DataTypes
importIO_subfields = {
    'STRING':[],
    'CURRENCY': ['_currency', '_source'],
    'INT': ['_source'],
    'DOUBLE': ['_source'],
    'LANG': ['_source'],
    'COUNTRY': ['_source'],
    'BOOLEAN': ['_source'],
    'URL': ['_text', '_source', '_title'],
    'IMAGE': ['_alt', '_title', '_source'],
    'HTML':[],
    'MAP':[],
    'DATE': ['_source', '_utc'], # seems to have disappeared in new versions
}

def convert_type(importIO_type):
    return {'BOOLEAN':'boolean', 'DOUBLE':'double'}.get(importIO_type,'string')

def convert_schema(import_io_schema):
    result = []
    for col in import_io_schema:
        result.append({'name':col['name'],'type':convert_type(col['type'])})
        for subfield in importIO_subfields[col['type']]:
            result.append({'name':col['name']+'/'+subfield, 'type':'string'})
    return result

def run(build_query):
    """Used by recipes (see them for example use).
    Do one API call per line of input dataset, write results to output dataset.
    Overwrite output schema."""
    input  = dataiku.Dataset(get_input_names_for_role ('input_dataset')[0])
    output = dataiku.Dataset(get_output_names_for_role('output_dataset')[0])
    output_writer = output.get_writer()
    api_url = get_recipe_config()['api_url']
    parsed_api_url = urlparse.urlparse(api_url)
    parsed_api_query = urlparse.parse_qs(parsed_api_url.query)
    delay_between_calls = int(get_recipe_config()['delay_between_api_calls'])

    output_schema = None
    input_cols_to_drop = []
    for in_row in input.iter_rows(log_every=10):
        query = build_query(in_row, parsed_api_query['_apikey'][0])
        request_url = urlparse.urlunparse((
            parsed_api_url.scheme, parsed_api_url.netloc, parsed_api_url.path, parsed_api_url.params,
            query, parsed_api_url.fragment))
        try:
            response = requests.get(request_url)
            json = response.json()
        except Exception as e:
            logger.error('request to import.io failed')
            logger.error(e)
            logger.error('response was: {}'.format(response))
            raise ValueError
        if 'error' in json:
            logger.error("response: {}".format(json))
            raise Exception(json['error'])
        for result_line in json['results']:
            if not output_schema:
                logger.error("Setting schema")
                input_schema_names = frozenset([e['name'] for e in input.read_schema()])
                output_schema = input.read_schema()
                print(')))))))))')
                print(json)
                print(json.keys())
                for col in convert_schema(json['outputProperties']):
                    if col['name'] in input_schema_names:
                        logger.error("Warning: input col "+col['name']+" will be overwritten by output col with same name.")
                        input_cols_to_drop.append(col['name'])
                    else:
                        output_schema.append(col)
                output.write_schema(output_schema)
                sys.stdout.flush()
            # Write downloaded data to output
            out_row = {k:v for k,v in in_row.items() if k not in input_cols_to_drop}
            out_row.update(result_line)
            output_writer.write_row_dict(out_row)
        time.sleep(delay_between_calls)

    output_writer.close()
