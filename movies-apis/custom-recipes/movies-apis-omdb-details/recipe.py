import pandas as pd
import requests
import dataiku
from dataiku.customrecipe import *

input_dataset = dataiku.Dataset(get_input_names_for_role('input_dataset')[0])

lookup_col = get_recipe_config().get('title_col','')
lookup_name = 'title_queried'
if lookup_col != '':
    use_id = False
else:
    use_id = True
    lookup_col = get_recipe_config().get('imdb_id_col','')
    lookup_name = 'IMDb_id_queried'
    if lookup_col == '':
        raise Exception('Please provide either a column containing titles or a column containing IMDb ids.')

base_query = 'http://www.omdbapi.com/?' \
    + "tomatoes=true" \
    + {
        "all"    : "",
        "movie"  : "&type=movie",
        "series" : "&type=series",
        "episode": "&type=episode",
      }[get_recipe_config()['type']]
    # y year of relase, plot={short,full}

output_dataset = dataiku.Dataset(get_output_names_for_role('output_dataset')[0])
output_writer = output_dataset.get_writer()

def write_output_schema(sample_line):
    print "setting schema"
    output_schema = [
        {'name':lookup_name,         'type':'string'},
        {'name':'Title',             'type':'string'},
        {'name':'imdbID',            'type':'string'},
        {'name':'imdbRating',        'type':'double'},
        {'name':'imdbVotes',         'type':'bigint'},
        {'name':'Metascore',         'type':'bigint'},
        {'name':'tomatoConsensus',   'type':'string'},
        {'name':'tomatoFresh',       'type':'bigint'},
        {'name':'tomatoImage',       'type':'string'},
        {'name':'tomatoMeter',       'type':'bigint'},
        {'name':'tomatoRating',      'type':'double'},
        {'name':'tomatoReviews',     'type':'bigint'},
        {'name':'tomatoRotten',      'type':'bigint'},
        {'name':'tomatoUserMeter',   'type':'bigint'},
        {'name':'tomatoUserRating',  'type':'double'},
        {'name':'tomatoUserReviews', 'type':'bigint'},
        {'name':'Actors',            'type':'string'},
        {'name':'Director',          'type':'string'},
        {'name':'Writer',            'type':'string'},
        {'name':'Awards',            'type':'string'},
        {'name':'BoxOffice',         'type':'string'},
        {'name':'Country',           'type':'string'},
        {'name':'Genre',             'type':'string'},
        {'name':'Language',          'type':'string'},
        {'name':'Plot',              'type':'string'},
        {'name':'Poster',            'type':'string'},
        {'name':'Production',        'type':'string'},
        {'name':'Rated',             'type':'string'},
        {'name':'Released',          'type':'string'},
        {'name':'Year',              'type':'bigint'},
        {'name':'DVD',               'type':'string'},
        {'name':'Runtime',           'type':'bigint'},
        {'name':'Type',              'type':'string'},
        {'name':'Website',           'type':'string'},
    ]
    known_keys = frozenset([e['name'] for e in output_schema])
    for key,v in sample_line.items():
        if key not in known_keys:
            output_schema.append({'name':key, 'type':'string'})
    output_dataset.write_schema(output_schema)
output_schema_set = output_dataset.read_schema(raise_if_empty=False) != []

results_notFound = []
for row in input_dataset.iter_rows(log_every=10):
    lookup = row[lookup_col]
    print "looking up", lookup.encode('utf-8')
    query = base_query + ('&i=' if use_id else '&t=') + lookup.encode('utf-8')
    movie = requests.get(query).json()
    if movie['Response'] == 'True':
        # some obvious cleaning:
        del movie['Response']
        movie['imdbVotes'] = movie['imdbVotes'].replace(',','')
        for col in ['Actors', 'Country', 'Genre', 'Language', 'Writer']:
            movie[col] = '[' + movie[col].replace(', ',',') + ']'
        if movie['Runtime'].endswith(' min'):
            movie['Runtime'] = movie['Runtime'][:-len(' min')]
        for col in ['Poster','Website', 'tomatoConsensus','tomatoImage']:
            if movie[col] == 'N/A': del movie[col]
        for col in ['Metascore', 'Runtime', 'Year', 'imdbVotes',
                    'tomatoFresh', 'tomatoMeter', 'tomatoReviews', 'tomatoRotten',
                    'tomatoUserMeter', 'tomatoUserReviews']:
            try:
                if movie[col] == 'N/A': del movie[col]
                else: movie[col] = int(movie[col])
            except:
                print "cannot cast to int:", col, movie[col]
        for col in ['imdbRating', 'tomatoRating', 'tomatoUserRating']:
            try:
                if movie[col] == 'N/A': del movie[col]
                else: movie[col] = float(movie[col])
            except:
                print "cannot cast to float:", col, movie[col]

        movie[lookup_name] = row[lookup_col]
        if not output_schema_set:
            write_output_schema(movie)
            output_schema_set = True
        output_writer.write_row_dict(movie)
    else:
        print 'Error'
        results_notFound.append({lookup_name: lookup, 'error': movie['Error']})
        assert movie['Response'] == 'False'
output_writer.close()

if get_output_names_for_role('movies_not_found'):
    notFound_dataset = dataiku.Dataset(get_output_names_for_role('movies_not_found')[0])
    notFound_dataset.write_with_schema(pd.DataFrame(results_notFound))
