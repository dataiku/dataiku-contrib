# -*- coding: utf-8 -*-

import logging
import numpy as np
from fasttext import load_model

import dataiku
from dataiku.customrecipe import *
from dataiku import pandasutils as pdu
from dataiku.customrecipe import get_recipe_resource

from preprocessing_utils import clean_text


logging.basicConfig(format='[PLUGIN RECIPE LOG] %(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(dataset_name)


recipe_config = get_recipe_config()
text_column_name = recipe_config.get('text_column_name', None)
if text_column_name == None:
    raise ValueError("You did not choose a text column.")
sentiment_scale = recipe_config.get('sentiment_scale')
output_probabilities = recipe_config.get('output_confidence', False)


#############################
# Load FastText Model
#############################

model = load_model(
    os.path.join(
        get_recipe_resource(),
        "fasttext",
        "sentiment_analysis",
        "amazon_review_polarity.ftz" if sentiment_scale == 'binary' else "amazon_review_full.ftz"
    )
)


#############################
# Score
#############################


# TODO reduce chunck size? Names entity recognition did that
CHUNK_SIZE = 10000

logger.info("Start chunk-processing of input dataset.".format(dataset_name))

n_lines = 0
for chunk_idx, df in enumerate(input_dataset.iter_dataframes(chunksize=CHUNK_SIZE)):

    # TODO output the initial text, not the clean ones
    texts = df[text_column_name].apply(lambda s: clean_text(str(s)).decode('utf-8')).values
    predicted_scores, confidence_list = model.predict(list(texts))

    # Post-process predicted Sentiment
    predicted_scores = np.array([int(v[0].split('__')[-1]) for v in predicted_scores])
    if sentiment_scale == 'binary':
        predicted_scores += -1  # polarity model predicts 1/2 instead of 0/1
    # TODO fix that, confidence_list is a list, not an np array, maybe fasttext update broke stuff...
    # confidence_list = confidence_list.ravel()

    if chunk_idx==0:
        # Compute new column names
        new_cols = ["predicted_scores", "predicted_sentiment"]
        if output_probabilities:
            new_cols.append("prediction_confidence")

        for i, column in enumerate(new_cols):
            if column in df.columns:
                j = 1
                while column + "_{}".format(j) in df.columns:
                    j += 1
                new_cols[i] = column + "_{}".format(j)

    # Add prediction to output dataframe
    df[new_cols[0]] = predicted_scores
    if sentiment_scale == 'binary':
        df[new_cols[1]] = [
            "positive" if p==1
            else "negative" for p in predicted_scores]
    else:
        df[new_cols[1]] = [
            "highly negative" if p==1
            else "negative" if p==2
            else "neutral" if p==3
            else "positive" if p==4
            else "highly positive" if p==5
            else "" for p in predicted_scores]

    if output_probabilities:
        df[new_cols[2]] = np.array(confidence_list) / float(max(confidence_list))

    # Append dataframe to output Dataset
    if chunk_idx==0:
        output_dataset.write_schema_from_dataframe(df)
        writer = output_dataset.get_writer()
        writer.write_dataframe(df)
    else:
        writer.write_dataframe(df)

    n_lines += len(df)
    logger.info("Finished processing {} lines".format(n_lines))
writer.close()
