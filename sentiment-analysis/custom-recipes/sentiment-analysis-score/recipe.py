# -*- coding: utf-8 -*-

import dataiku
from dataiku.customrecipe import *
from dataiku import pandasutils as pdu
from dataiku.customrecipe import get_recipe_resource

from preprocessing_utils import clean_text
from helpers import detect_language_using_fasttext

import numpy as np
from fastText import load_model


#############################
# Input data
#############################

input_dataset = get_input_names_for_role('input_dataset')[0]
df = dataiku.Dataset(input_dataset).get_dataframe()


#############################
# Recipe Parameters
#############################

recipe_config = get_recipe_config()


text_column_name = recipe_config.get('text_column_name', None)
if text_column_name == None:
    raise ValueError("You did not choose a text column.")

    
texts = df[text_column_name].apply(
    lambda s: clean_text(str(s)).decode('utf-8')).values

    
detect_language = bool(recipe_config.get('detect_language', False))
if not detect_language:
    text_language = recipe_config.get('text_language', None)
    if text_language is None:
        raise ValueError("You should either choose a language or turn on automatic language identification.")
else:
    text_language = "sentence_level"


output_probabilities = bool(recipe_config.get('output_confidence', False))


#############################
# Load Models
#############################

en_model = load_model(
    os.path.join(
        get_recipe_resource(),
        "fasttext",
        "sentiment_analysis",
        "amazon_review_polarity.ftz"
    )
)

fr_model = load_model(
    os.path.join(
        get_recipe_resource(),
        "fasttext",
        "sentiment_analysis",
        "allocine_marmiton_tripadvisor.ftz"
    )
)

    
language_identification_model = load_model(
    os.path.join(
        get_recipe_resource(),
        "fasttext",
        "language_detection",
        "lid.176.ftz"
    )
)


#############################
# Score
#############################

if text_language == 'sentence_level':
    
    predicted_polarities, confidence_list = [], []
    for text in texts:

        language, _ = language_identification_model.predict(text)
        language = language[0].split("__label__")[-1]
        if language == 'fr':
            model = fr_model
        else:
            model = en_model

        polarity, confidence = model.predict(text)
        polarity = int(polarity[0].split("__label__")[-1])
        if language != 'fr':
            polarity += -1  # English model predicts 1/2 instead of 0/1
        confidence = confidence[0]
        
        predicted_polarities.append(polarity)
        confidence_list.append(confidence)

else:

    if text_language == 'english':
        model = en_model
    elif text_language == 'french':
        model = fr_model
    else:
        NotImplementedError("The plugin only supports French and English languages.")

    predicted_polarities, confidence_list = model.predict(list(texts))
    predicted_polarities = np.array([int(v[0].split('__')[-1]) for v in predicted_polarities])
    if text_language == "english":
        predicted_polarities += -1  # English model predicts 1/2 instead of 0/1

    confidence_list = confidence_list.ravel()


# Add new columns to output dataset

new_cols = ["predicted_polarity", "predicted_sentiment"]
if output_probabilities:
    new_cols.append("prediction_confidence")

for i, column in enumerate(new_cols):
    if column in df.columns:
        j = 1
        while column + "_{}".format(j) in df.columns:
            j += 1
        new_cols[i] = column + "_{}".format(j)

df[new_cols[0]] = predicted_polarities
df[new_cols[1]] = ["positive" if p==1 else "negative" for p in predicted_polarities]
if output_probabilities:
    df[new_cols[2]] = np.array(confidence_list) / float(max(confidence_list))


#############################
# Output
#############################

output_dataset = get_output_names_for_role('output_dataset')[0]
dataiku.Dataset(output_dataset).write_with_schema(df)

