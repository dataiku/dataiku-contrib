# -*- coding: utf-8 -*-

import dataiku
from dataiku.customrecipe import *
import logging
import json
import os
import requests
import shelve

logging.basicConfig(level=logging.INFO, format='email-tester-mailgun plugin - %(levelname)s - %(message)s')
#logging.basicConfig(level=logging.DEBUG, format='email-tester-mailgun plugin - %(levelname)s - %(message)s')

# Plugin version
PLUGIN_VERSION = '0.0.1'
logging.info('version: %s' % PLUGIN_VERSION)

# Get handles on datasets
contacts = dataiku.Dataset(get_input_names_for_role('contacts')[0])
output = dataiku.Dataset(get_output_names_for_role('output')[0])

# Read configuration
config = get_recipe_config()
email_column = config.get('email_column', None)
cache_folder = config.get('cache_folder', None)
api_key = config.get('api_key', None)

# Verification existing inputs
if not email_column or not cache_folder or not api_key:
    raise Exception("Missing inputs")

# Prepare schema
output_schema = list(contacts.read_schema())
output_schema.append({'name':'email_is_valid', 'type':'STRING'})
output_schema.append({'name':'email_mailgun_details', 'type':'STRING'})
output.write_schema(output_schema)

# Verification of the email column
columns_names = [col.get('name') for col in output_schema]
if email_column not in columns_names:
    raise Exception("Not able to find the '%s' column" % email_column)

# Verification of a valid internet connection
# TODO

# Cache directory
if not os.path.isdir(cache_folder):
    os.makedirs(cache_folder)

# Cache
filename = 'email-tester-mailgun-cache'
cache_file = os.path.join(cache_folder, filename)
cache = shelve.open(cache_file, writeback = True)
cache_plugin_version = cache['plugin_version'] if 'plugin_version' in cache else None
logging.info('cache file: %s' % cache_file)
logging.debug('cache: %s' % str(cache))

# Function to compare version numbers
def versiontuple(v):
    return tuple(map(int, (v.split("."))))

# Cleaning cache if old version
if cache_plugin_version and versiontuple(cache_plugin_version) < versiontuple(PLUGIN_VERSION):
    logging.info('clearing cache')
    cache["emails"] = {}

# Preparing cache
cache['plugin_version'] = PLUGIN_VERSION
if "emails" not in cache.keys():
    cache["emails"] = {}

# Preparing API
API_URL = "https://api.mailgun.net/v3/address/validate"
API_AUTH = ("api", api_key)

# Function to make a call to Mailgun API
def api_call(email):
    r = requests.get(API_URL,
                     auth=API_AUTH,
                     params={"address": email})
    if r.status_code != requests.codes.ok :
        logging.info("Error in the API call for %s. Status code: %s" % (email, r.status_code) )
        return None
    else:
        return r.json()

# Email testing and writing results one by one

writer = output.get_writer()

i = 0
for contact in contacts.iter_rows():

    i += 1
    contact = dict(contact)
    email = contact[email_column]

    if email is not None and len(email) > 0:

        if email in cache["emails"].keys():
            result = cache["emails"][email]
            logging.debug("%i: Email already in cache: %s -> %s" % (i, email, result))
        else:
            result = api_call(email)
            logging.debug("%i: Call API for email %s -> %s" % (i, email, result))
            if result is not None:
                cache["emails"][email] = result

        if result is not None and 'is_valid' in result:
            contact['email_is_valid'] = result.get('is_valid')
        else:
            contact['email_is_valid'] = ''

        contact['email_mailgun_details'] = json.dumps(result)

    writer.write_row_dict(contact)
    

logging.debug('cache: %s' % str(cache))

writer.close()
cache.close()
