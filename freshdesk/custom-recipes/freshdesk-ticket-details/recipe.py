# -*- coding: utf-8 -*-
import dataiku, base64, logging, requests, time, json
import pandas as pd
from dataiku.customrecipe import *

input_name = get_input_names_for_role ('input_dataset' )[0]
tickets = dataiku.Dataset(input_name).get_dataframe()

key = get_plugin_config()['api_key']
endpoint = get_plugin_config()['endpoint']
def fetch_ticket(id):
    print ("Fetching ticket %i" % id)
    headers = {'Authorization': "Basic " + base64.b64encode(key+':X')}
    path = '/helpdesk/tickets/%i.json' % id
    for i in range(3):
        try:
            r = requests.get(endpoint + path, headers=headers)
            if r.status_code == 403 and "Retry-After" in r.headers:
                seconds = int(r.headers["Retry-After"])
                print "Rate limit reached. Sleeping for %i seconds, as asked by Freshdesk." % seconds
                time.sleep(seconds)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print e
            print r.headers
    raise

details = tickets.display_id.apply(lambda i: pd.Series(fetch_ticket(i)['helpdesk_ticket']))
for c in [c for c in details.columns if c not in tickets.columns]:
    tickets[c] = details[c].map(lambda v:json.dumps(v) if type(v) in [list,dict] else v)

output_name  = get_output_names_for_role('output_dataset')[0]
dataiku.Dataset(output_name).write_with_schema(tickets)
