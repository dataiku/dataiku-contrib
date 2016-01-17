# -*- coding: utf-8 -*-

import dataiku
from dataiku.customrecipe import *
import logging
import json
import os
import requests
import shelve
import re
import dns.resolver #http://www.dnspython.org

logging.basicConfig(level=logging.INFO, format='email-tester-locally plugin - %(levelname)s - %(message)s')
#logging.basicConfig(level=logging.DEBUG, format='email-tester-locally plugin - %(levelname)s - %(message)s')

# Plugin version
PLUGIN_VERSION = '0.0.0'
logging.info('version: %s' % PLUGIN_VERSION)

# Get handles on datasets
contacts = dataiku.Dataset(get_input_names_for_role('contacts')[0])
output = dataiku.Dataset(get_output_names_for_role('output')[0])

# Read configuration
config = get_recipe_config()
email_column = config.get('email_column', None)
cache_folder = config.get('cache_folder', None)
verification_level = int(config.get('verification_level', 1))

logging.info('verification_level: %i' % verification_level)

# Verification existing inputs
if not email_column or not cache_folder:
    raise Exception("Missing inputs")

# Prepare schema
output_schema = list(contacts.read_schema())
output_schema.append({'name':'email_is_valid', 'type':'STRING'})
output_schema.append({'name':'email_error', 'type':'STRING'})
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
filename = 'email-tester-locally-cache'
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
    cache["domains"] = {}

# Preparing cache
cache['plugin_version'] = PLUGIN_VERSION
if "domains" not in cache.keys():
    cache["domains"] = {}

# Getting disposable email domains from https://github.com/ivolo/disposable-email-domains
if verification_level > 1:
    r = requests.get('https://rawgit.com/ivolo/disposable-email-domains/master/index.json')
    disposable_domains = r.json()
    if r.status_code >= 300 or not isinstance(disposable_domains, list) or len(disposable_domains) < 50:
        raise Exception("Error when getting disposable emails domains")
else:
    disposable_domains = None

# Email format validation
EMAIL_REGEX = re.compile(r"^[A-Za-z0-9\.\+_-]+@[A-Za-z0-9\._-]+\.[a-zA-Z]{2,10}$")
def email_test_regex(email):
    if not email or len(email) == 0:
        return False
    elif EMAIL_REGEX.match(email):
        return True
    else:
        return False

# Email test disposable domain
def email_test_disposable(email):
    if not email or len(email) == 0 or disposable_domains is None:
        return False
    domain = email.split("@")[-1]
    return domain in disposable_domains

# Email DNS MX lookup
# (with caching)
def email_test_mx_record(email):
    global cache

    if not email or len(email) == 0:
        return False

    email = str(email)
    domain = email.split("@")[-1]

    # if cache, returns cache result
    if domain in cache["domains"].keys():
        logging.debug("Domain already in cache: %s -> %s" % (domain, cache["domains"][domain]))
        return cache["domains"][domain]

    try:
        result = len(dns.resolver.query(domain, 'MX')) > 0
    except Exception as e:
        result = False

    cache["domains"][domain] = result
    return result


# Email testing and writing results one by one

writer = output.get_writer()

i = 0
for contact in contacts.iter_rows():

    i += 1
    contact = dict(contact)
    email = contact[email_column]

    if not email_test_regex(email):
        contact['email_is_valid'] = 'False'
        contact['email_error'] = 'Wrong format'

    elif verification_level > 1 and email_test_disposable(email):
        contact['email_is_valid'] = 'False'
        contact['email_error'] = 'Disposable domain'

    elif verification_level > 2 and not email_test_mx_record(email):
        contact['email_is_valid'] = 'False'
        contact['email_error'] = 'No MX record for domain'

    else:
        contact['email_is_valid'] = 'True'
        contact['email_error'] = ''

    writer.write_row_dict(contact)
    logging.debug("%i: %s -> validity: %s (%s)" % (i, email, contact['email_is_valid'], contact['email_error']))

logging.debug('cache: %s' % str(cache))

writer.close()
cache.close()
