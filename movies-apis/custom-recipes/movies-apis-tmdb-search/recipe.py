# -*- coding: utf-8 -*-
import pandas as pd
import tmdbsimple as tmdb
import requests, time
import dataiku
from dataiku.customrecipe import *

tmdb.API_KEY = get_plugin_config()['tmdb_api_key']

input_dataset = dataiku.Dataset(get_input_names_for_role('input_dataset')[0])
title_col = get_recipe_config().get('title_col')
release_date_col = get_recipe_config().get('release_date_col',None)
results = []
results_notFound = []

for row in input_dataset.iter_rows(log_every=10):
    title = row[title_col]
    print "looking up", title.encode('utf-8')
    try:
        response = tmdb.Search().movie(query=title)["results"]
        time.sleep(0.05)
    except requests.exceptions.HTTPError as e:
        print 'Error:', e
        results_notFound.append({'title_queried': title, 'error': e})
        continue
    if len(response) == 0:
        print 'Error: no matches'
        results_notFound.append({'title_queried': title, 'error': 'no matches'})
        continue
    movies = pd.DataFrame(response)
    # choose best match
    id_tmdb = movies.sort_index(by="popularity", ascending=0).ix[0,"id"]
    if release_date_col and row[release_date_col]: # choose rather based on date diff
        movies['date_diff'] = abs(pd.to_datetime(movies['release_date']) - pd.to_datetime(row[release_date_col]))
        if movies["date_diff"].min().days <= 365 *2:
            id_tmdb = movies.sort_index(by="date_diff").ix[0,"id"]
    try:
        movie = tmdb.Movies(id_tmdb).info()
    except requests.exceptions.HTTPError as e:
        print 'Error after getting id ' + str(id_tmdb) + ': ' + e
        results_notFound.append({'': title, 'error': '(on id '+id_tmdb+')' + e})
        continue
    movie['title_queried'] = title
    results.append(movie)

output_dataset   = dataiku.Dataset(get_output_names_for_role('output_dataset'  )[0])
output_dataset.write_with_schema(pd.DataFrame(results))
if get_output_names_for_role('movies_not_found'):
    notFound_dataset = dataiku.Dataset(get_output_names_for_role('movies_not_found')[0])
    notFound_dataset.write_with_schema(pd.DataFrame(results_notFound))
