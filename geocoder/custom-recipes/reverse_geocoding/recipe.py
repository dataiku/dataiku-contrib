# -*- coding: utf-8 -*-

import dataiku
from dataiku.customrecipe import *
from dataiku import pandasutils as pd
from cache_handler import CacheHandler

import geocoder
import pandas as pd, numpy as np
import logging
import errno, os, sys

# Logging setup
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def get_config():
    config = {}
    config['input_ds'] = dataiku.Dataset(get_input_names_for_role('input_ds')[0])
    config['output_ds'] = dataiku.Dataset(get_output_names_for_role('output_ds')[0])
        
    for param in ['lat_column', 'lng_column', 'provider', 'cache_enabled', 'api_key', 'here_app_id', 'here_app_code', 'google_client', 'google_client_secret']:
        config[param] = get_recipe_config().get(param, None)
    
    config['batch_enabled'] = get_recipe_config().get('batch_enabled', False) \
                                and (config['provider'] == 'bing')
    config['batch_size'] = get_recipe_config().get('batch_size_bing', 50)

    config['features'] = []
    prefix = get_recipe_config().get('column_prefix', '')

    for feature in ['address', 'city', 'postal', 'state', 'country']:
        if get_recipe_config().get(feature, False):
            config['features'].append({'name': feature, 'column': prefix + feature})

    if get_plugin_config().get('cache_location', 'original') == 'original':
        config['cache_location'] = os.environ["DIP_HOME"] + '/caches/plugins/geocoder/reverse'
    else:
        config['cache_location'] = get_plugin_config().get('cache_location_custom', '')

    config['cache_size'] = get_plugin_config().get('reverse_cache_size', 1000) * 1000
    config['cache_eviction'] = get_plugin_config().get('reverse_cache_policy', 'least-recently-stored')
    
    if len(config['features']) == 0:
        raise AttributeError('Please select at least one feature to extract.')
    
    if config['provider'] is None:
        raise AttributeError('Please select a geocoding provider.')
        
    return config

def get_geocode_function(config):
    provider_function = getattr(geocoder, config['provider']) 

    if config['provider'] == 'here':
        return lambda lat, lng: provider_function([lat, lng], method='reverse', app_id=config['here_app_id'], app_code=config['here_app_code'])
    elif config['provider'] == 'google':
        return lambda lat, lng: provider_function([lat, lng], method='reverse', key=config['api_key'], client=config['google_client'], client_secret=config['google_client_secret'])
    elif config['batch_enabled']:
        return lambda locations: provider_function(locations, method='batch_reverse', key=config['api_key'])
    else:
        return lambda lat, lng: provider_function([lat, lng], method='reverse', key=config['api_key'])

def is_empty(val):
    if isinstance(val, float):
        return np.isnan(val)
    else:
        return not val

def perform_geocode(df, config, fun, cache):
    lat = df[config['lat_column']]
    lng = df[config['lng_column']]
    res = {'address': None, 'city': None, 'postal': None, 'state': None, 'country': None}

    try:
        if any([is_empty(df[f['column']]) for f in config['features']]):
            res = cache[(lat, lng)]
        else:
            for f in config['features']:
                res[f['name']] = df[f['column']]

    except KeyError:
        try:
            out = fun(lat, lng)
            if not out.address and not out.city and not out.postal and not out.state and not out.country:
                raise Exception('Failed to retrieve coordinates')

            for feature in res.keys():
                val = getattr(out, feature)
                res[feature] = val.encode('utf-8', 'ignore') if val and type(val) == unicode else val

            cache[(lat, lng)] = res
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % ((lat, lng), e))

    formatted_res = []
    for feature in config['features']:
        formatted_res.append(res[feature['name']])

    return formatted_res

def perform_geocode_batch(df, config, fun, cache, batch):
    results = []
    try:
        results = fun(zip(*batch)[1])
    except Exception as e:
        logging.error("Failed to geocode the following batch: %s (%s)" % (batch, e))

    for out, orig in zip(results, batch):
        try:
            if not out.address and not out.city and not out.postal and not out.state and not out.country:
                raise Exception('Failed to retrieve coordinates')

            res = {'address': None, 'city': None, 'postal': None, 'state': None, 'country': None}

            for feature in res.keys():
                val = getattr(out, feature)
                res[feature] = val.encode('utf-8', 'ignore') if val and type(val) == unicode else val

            i, loc = orig
            cache[(loc[0], loc[1])] = res

            for feature in config['features']:
                df.loc[i, feature['column']] = res[feature['name']]

        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (loc, e))
    
    
def main():
    config = get_config()
    geocode_function = get_geocode_function(config)
    input_df = config['input_ds'].get_dataframe()

    writer = None

    try:
        with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], \
                          size_limit=config['cache_size'], eviction_policy=config['cache_eviction']) as cache:
            for current_df in config['input_ds'].iter_dataframes(chunksize=max(10000, config['batch_size'])):
                columns = current_df.columns.tolist()

                columns_to_append = [f['column'] for f in config['features'] if not f['column'] in columns]
                if columns_to_append:
                    index = max(columns.index(config['lat_column']), columns.index(config['lng_column']))
                    current_df = current_df.reindex(columns = columns[:index + 1] + columns_to_append + columns[index + 1:], copy=False)

                if not config['batch_enabled']:
                    results = zip(*current_df.apply(perform_geocode, axis=1, args=(config, geocode_function, cache)))

                    for feature, result in zip(config['features'], results):
                        current_df[feature['column']] = result

                else:
                    batch = []

                    for i, row in current_df.iterrows():
                        if len(batch) == config['batch_size']:
                            perform_geocode_batch(current_df, config, geocode_function, cache, batch)
                            batch = []

                        lat = row[config['lat_column']]
                        lng = row[config['lng_column']]

                        try:
                            if any([is_empty(row[f['column']]) for f in config['features']]):
                                res = cache[(lat, lng)]
                            else:
                                res = {}
                                for f in config['features']:
                                    res[f['name']] = row[f['column']]

                            for feature in config['features']:
                                current_df.loc[i, feature['column']] = res[feature['name']]

                        except KeyError as e:
                            batch.append((i, (lat, lng)))
                    
                    if len(batch) > 0:
                        perform_geocode_batch(current_df, config, geocode_function, cache, batch)


                # First loop, we write the schema before creating the dataset writer
                if writer is None:
                    config['output_ds'].write_schema_from_dataframe(current_df)
                    writer = config['output_ds'].get_writer()

                writer.write_dataframe(current_df)
    finally:
        if writer is not None:
            writer.close()

main()
