# coding=utf-8

u"""Geocoding plugin for Dataiku Science Studio (DSS)

It uses (by default) the geocoding server from: http://adresse.data.gouv.fr

It will also work on any private instance of https://github.com/addok/addok

This plugin was developped by the Ministère de l’Intérieur
in the context of the program Entrepreneurs d’Intérêt Général 2017
"""

from concurrent import futures
import dataiku
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config
import itertools
import logging
import pandas as pd
import requests
import StringIO

# We read the addresses from the input dataset
# And write the coordinates in the output dataset
input_name = get_input_names_for_role('input')[0]
input_dataset = dataiku.Dataset(input_name)

output_name = get_output_names_for_role('output')[0]
output_dataset = dataiku.Dataset(output_name)

# All the variables when building a request
server_address = get_recipe_config()['server_address']
columns = get_recipe_config()['columns']
post_code = get_recipe_config().get('post_code', None)
city_code = get_recipe_config().get('city_code', None)
lines_per_request = int(get_recipe_config()['lines_per_request'])
concurent_requests = int(get_recipe_config()['concurent_requests'])
http_proxy = get_recipe_config().get('http_proxy', None)
timeout = int(get_recipe_config()['timeout'])
prefix = get_recipe_config().get('prefix', None)
error = get_recipe_config().get('error_col', None)
i = 0


def datas():
    """Returns the columns composing the address"""
    result = {'columns': columns}
    cols = list(columns)

    if post_code:
        result['postcode'] = post_code
        cols.append(post_code)

    if city_code:
        result['citycode'] = city_code
        cols.append(city_code)

    return (result, cols)


def adresse_submit(df):
    """Does the actual request to the geocoding server"""
    global i
    verbosechunksize = 2000
    string_io = StringIO.StringIO()
    i += lines_per_request
    if (i % verbosechunksize) == 0:
        logging.info("geocoding chunk %r to %r", i-verbosechunksize, i)

    data, cols = datas()
    df[cols].to_csv(string_io, encoding="utf-8", index=False)

    kwargs = {
        'data': data,
        'files': {'data': string_io.getvalue()},
        'timeout': timeout,
        'url': "{}/search/csv".format(server_address)
    }

    if http_proxy:
        kwargs['proxies'] = {'http': http_proxy}

    response = requests.post(**kwargs)

    if error:
        error_col = 'result_{}'.format(error)
    else:
        error_col = None

    if response.status_code == 200:
        content = StringIO.StringIO(response.content.decode('utf-8-sig'))
        result = pd.read_csv(content, dtype=object)
        if error_col:
            result[error_col] = None
        result = result.rename(columns={'longitude': 'result_longitude',
                                        'latitude': 'result_latitude'})

        # We only keep the new columns to avoid modifying the schema
        diff = result.axes[1].difference(df.axes[1])

        for new_column in diff:
            if new_column[0:7] == "result_":
                df[new_column.replace("result_", prefix)] = result[new_column]

    else:
        logging.warning("Chunk %r to %r: no valid response",
                        i-lines_per_request, i)
        df['result_score'] = -1
        if error_col:
            df["{}{}".format(prefix, error)] = "HTTP Status: {}".format(response.status_code)

    return df


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)


# We make a first run with a sample to have a valid schema to build a writer
small = input_dataset.get_dataframe(sampling='head',
                                    limit=1,
                                    infer_with_pandas=False)

initial_index = small.axes[1]
geocoded = adresse_submit(small)
output_index = geocoded.axes[1]

if '{}longitude'.format(prefix) not in output_index:
    raise Exception('Geocoding failed: unable to make a sample request')

schema = input_dataset.read_schema()

floats = [prefix + column for column in ['longitude', 'latitude', 'score']]
for column in output_index.difference(initial_index):
    if column in floats:
        schema.append({'name': column, 'type': 'float'})
    else:
        schema.append({'name': column, 'type': 'string'})

output_dataset.write_schema(schema)
writer = output_dataset.get_writer()

dataset_iter = input_dataset.iter_dataframes(chunksize=lines_per_request,
                                             infer_with_pandas=False)

with futures.ThreadPoolExecutor(max_workers=concurent_requests) as executor:
    for chunks in grouper(dataset_iter, 10 * concurent_requests):
        j = 0
        for s in executor.map(adresse_submit, chunks):
            j += lines_per_request
            try:
                writer.write_dataframe(s)
            except Exception as exc:
                logging.warning("chunk %r to %r generated an exception: %r\n%r",
                                j-lines_per_request, j, exc, s)

writer.close()
