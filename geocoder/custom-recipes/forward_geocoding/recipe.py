# -*- coding: utf-8 -*-

import dataiku
from dataiku.customrecipe import *
from dataiku import pandasutils as pd
from cache_handler import CacheHandler

import geocoder
import pandas as pd, numpy as np
import logging
import errno, os

# Logging setup
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def get_config():
    config = {}
    config['input_ds'] = dataiku.Dataset(get_input_names_for_role('input_ds')[0])
    config['output_ds'] = dataiku.Dataset(get_output_names_for_role('output_ds')[0])
        
    for param in ['address_column', 'cache_enabled', 'provider', 'api_key', 'here_app_id', 'here_app_code', 'google_client', 'google_client_secret']:
        config[param] = get_recipe_config().get(param, None)
    
    config['batch_enabled'] = get_recipe_config().get('batch_enabled', False) \
                                and (config['provider'] == 'bing' or config['provider'] == 'mapquest' or config['provider'] == 'uscensus')

    config['batch_size'] = {
        'bing': get_recipe_config().get('batch_size_bing', 50),
        'mapquest': 100,
        'uscensus': get_recipe_config().get('batch_size_uscensus', 1000)
    }.get(config['provider'], 0)

    config['smart_compute'] = get_recipe_config().get('smart_compute', True)

    if get_plugin_config().get('cache_location', 'original') == 'original':
        config['cache_location'] = os.environ["DIP_HOME"] + '/caches/plugins/geocoder/forward'
    else:
        config['cache_location'] = get_plugin_config().get('cache_location_custom', '')

    config['cache_size'] = get_plugin_config().get('forward_cache_size', 1000) * 1000
    config['cache_eviction'] = get_plugin_config().get('forward_cache_policy', 'least-recently-stored')
    
    prefix = get_recipe_config().get('column_prefix', '')
    for column_name in ['latitude', 'longitude']:
        config[column_name] = prefix + column_name
    
    if config['provider'] is None:
        raise AttributeError('Please select a geocoding provider.')
        
    return config

def get_geocode_function(config):
    provider_function = getattr(geocoder, config['provider']) 

    if config['provider'] == 'here':
        return lambda address: provider_function(address, app_id=config['here_app_id'], app_code=config['here_app_code'])
    elif config['provider'] == 'google':
        return lambda address: provider_function(address, key=config['api_key'], client=config['google_client'], client_secret=config['google_client_secret'])
    elif config['batch_enabled']:
        return lambda addresses: provider_function(addresses, key=config['api_key'], method='batch', timeout=30.0)
    else:
        return lambda address: provider_function(address, key=config['api_key'])

def is_empty(val):
    if isinstance(val, float):
        return np.isnan(val)
    else:
        return not val

def perform_geocode(df, config, fun, cache):
    address = df[config['address_column']]
    res = [None, None]

    try:
        if not config['smart_compute'] or all([is_empty(df[config[c]]) for c in ['latitude', 'longitude']]):
            res = cache[address]
        else:
            res = [df[config[c]] for c in ['latitude', 'longitude']]

    except KeyError:
        try:
            out = fun(address)
            if not out.latlng:
                raise Exception('Failed to retrieve coordinates')

            cache[address] = res = out.latlng
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (address, e))

    return res
    
def perform_geocode_batch(df, config, fun, cache, batch):
    try:
        results = fun(zip(*batch)[1])
    except Exception as e:
        logging.error("Failed to geocode the following batch: %s (%s)" % (batch, e))

    for res, orig in zip(results, batch):
        try:
            i, addr = orig
            cache[addr] = res.latlng

            df.loc[i, config['latitude']] = res.lat
            df.loc[i, config['longitude']] = res.lng
        except Exception as e:
            logging.error("Failed to geocode %s (%s)" % (addr, e))

if __name__ == '__main__':
    config = get_config()
    geocode_function = get_geocode_function(config)
    input_df = config['input_ds'].get_dataframe()

    writer = None

    try:
        with CacheHandler(config['cache_location'], enabled=config['cache_enabled'], \
                          size_limit=config['cache_size'], eviction_policy=config['cache_eviction']) as cache:
            for current_df in config['input_ds'].iter_dataframes(chunksize=max(10000, config['batch_size'])):
                columns = current_df.columns.tolist()
                if not all(config[c] in columns for c in ['latitude', 'longitude']):
                    index = columns.index(config['address_column'])
                    current_df = current_df.reindex(columns = \
                        columns[:index + 1] + [config['latitude'], config['longitude']] + columns[index + 1:], copy=False)

                if not config['batch_enabled']:
                    current_df[config['latitude']], current_df[config['longitude']] = \
                        zip(*current_df.apply(perform_geocode, axis=1, args=(config, geocode_function, cache)))

                else:
                    batch = []

                    for i, row in current_df.iterrows():
                        if len(batch) == config['batch_size']:
                            perform_geocode_batch(current_df, config, geocode_function, cache, batch)
                            batch = []

                        address = row[config['address_column']]
                        try:
                            if not config['smart_compute'] or all([is_empty(row[config[c]]) for c in ['latitude', 'longitude']]):
                                res = cache[address]
                            else:
                                res = [row[config[c]] for c in ['latitude', 'longitude']]

                            current_df.loc[i, config['latitude']] = res[0]
                            current_df.loc[i, config['longitude']] = res[1]
                        except KeyError:
                            batch.append((i, address))
                    
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
            