# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *

from sentence_embedding_utils import EmbeddingModel, clean_text, preprocess_and_compute_sentence_embedding

import os
import json
import numpy as np
from scipy.stats import wasserstein_distance
from scipy.spatial.distance import cosine, euclidean

import logging

FORMAT = '[SENTENCE EMBEDDING] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


##################################
# Input data
##################################

input_dataset = get_input_names_for_role('input_dataset')[0]
df = dataiku.Dataset(input_dataset).get_dataframe()

embedding_folder = get_input_names_for_role('embedding_folder')[0]
folder_path = dataiku.Folder(embedding_folder).get_path()


##################################
# Parameters
##################################

recipe_config = get_recipe_config()

text_column_names = []
for i in [1, 2]:
    column_name = recipe_config.get('text_column_{}'.format(i), None)
    if column_name is None:
        raise ValueError("You did not choose a text column {}.".format(i))
    text_column_names.append(column_name)
    
distance = recipe_config.get('distance', None)
if distance is None:
        raise ValueError("You did not choose a distance.")    

embedding_is_custom = recipe_config.get('embedding_is_custom', False)

aggregation_method = recipe_config.get('aggregation_method', None)

if aggregation_method is None:
    raise ValueError("You did not choose an aggregation method.")

elif aggregation_method == 'simple_average':
    smoothing_parameter, npc = None, None

elif aggregation_method == 'SIF':
    advanced_settings = recipe_config['advanced_settings']
    if advanced_settings:
        smoothing_parameter = float(recipe_config['smoothing_parameter'])
        npc = int(recipe_config['n_principal_components'])
    else:
        smoothing_parameter = 0.001
        npc = 1


##################################
# Loading embeddings
##################################

logger.info("Loading word embeddings from the input folder...")
embedding_model = EmbeddingModel(folder_path, embedding_is_custom)


##################################
# Computing sentence embeddings
##################################

logger.info("Computing sentence embeddings...")

embeddings_list = []
for name in text_column_names:

    sentence_embeddings = preprocess_and_compute_sentence_embedding(
        texts=df[name].values,
        embedding_model=embedding_model,
        method=aggregation_method,
        smoothing_parameter=smoothing_parameter,
        npc=npc)

    # Checking for existing columns with same name
    new_column_name = "{}-{}".format(name, aggregation_method)
    if new_column_name in df.columns:
        j = 1
        while new_column_name + "_{}".format(j) in df.columns:
            j += 1
        new_column_name += "_{}".format(j)

    # Adding a new column with computed embeddings
    if embedding_model.origin == "elmo":
        df[new_column_name] = [str(v) for v in sentence_embeddings]
    else:
        df[new_column_name] = [str(v.tolist()) for v in sentence_embeddings]

    embeddings_list.append(sentence_embeddings)
    
        
logger.info("Computed sentence embeddings.")

logger.info("Computing similarity scores...")

if distance == "cosine":
    distance_function = cosine

elif distance == "euclidian":
    distance_function = euclidian

elif distance == "absolute":
    def distance_function(x, y):
        x = np.array(x)
        y = np.array(y)
        return np.linalg.norm(x - y, ord=1)
    
elif distance == "wasserstein":
    distance_function = wasserstein_distance


# Computing distances between all couples of sentences
distances = np.fromiter(
    (distance_function(e1, e2)
     for (e1, e2) in zip(*embeddings_list)), float)
    
logger.info("Computed similarity scores.")

    
# Checking for existing columns with same name
new_column_name = "distance_{}-{}".format(*text_column_names)
if new_column_name in df.columns:
    j = 1
    while new_column_name + "_{}".format(j) in df.columns:
        j += 1
    new_column_name += "_{}".format(j)

# Adding a new column with computed embeddings
df[new_column_name] = distances


# Write recipe outputs
output_dataset = get_output_names_for_role('output_dataset')[0]
dataiku.Dataset(output_dataset).write_with_schema(df)
