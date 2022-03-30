import json
import logging
import requests


def process(row):
    story = get_story(row[params['sc_card_id_column']])
    return story['pull_requests']


def get_story(story_id):
    return execute_query("stories/" + str(story_id))


def execute_query(query):
    headers = {"Content-Type": "application/json"}

    endpoint_template = "https://api.app.shortcut.com/api/v3/{query}"
    endpoint = endpoint_template.format(query=query)
    logging.info("Fetching data from: {}".format(endpoint))

    with_token_template = "{endpoint}?token={token}"
    r = requests.get(with_token_template.format(endpoint=endpoint, token=plugin_params["api_token"]), headers=headers)
    r.raise_for_status()
    try:
        return json.loads(r.content)
    except Exception:
        logging.warn("Could not parse json from request content:\n" + r.content)
        raise
