# -*- coding: utf-8 -*-
import json
import logging
from collections import defaultdict
import pandas as pd
import spacy
from dataiku.customrecipe import *

logging.basicConfig(format='[NER RECIPE] %(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


#############################
# Loading SpaCy
#############################

recipe_config = get_recipe_config()
LANGUAGE = recipe_config.get('text_language_spacy', False)

try:
    nlp = spacy.load(LANGUAGE)
    logger.info("Successfully loaded SpaCy's {} model.".format(LANGUAGE))
except IOError:
    import sys
    from subprocess import Popen, PIPE

    logger.info(
        "SpaCy's {} model not found. Attempting an install...".format(LANGUAGE))

    # sys.executable returns the complete path to the python executable of the current process
    command = [sys.executable, "-m", "spacy", "download", LANGUAGE]

    logger.warning("Running the command: {}".format(' '.join(command)))
    p = Popen(command, stdout=PIPE, stderr=PIPE)
    output, err = p.communicate()
    for line in output.decode().split('\n'):
        logger.info(line)
    if err:
        logger.warning(err)

    try:
        nlp = spacy.load(LANGUAGE)
        logger.info("Successfully installed SpaCy's {} model.".format(LANGUAGE))
    except:
        raise Exception("Could not download SpaCy's model, probably because you don't have admin rights over the plugin code environment.")


#############################
# NER Function
#############################

def extract_entities(text_column, format):
    # Tag sentences
    docs = nlp.pipe(text_column.values)

    # Extract entities
    entity_df = pd.DataFrame()
    for doc in docs:
        df_row = defaultdict(list)
        for entity in doc.ents:
            df_row[entity.label_].append(entity.text)

        if format:
            df_row = {
                'sentence': doc.text,
                'entities': json.dumps(df_row)}
        else:
            for k, v in df_row.items():
                df_row[k] = json.dumps(v)
            df_row['sentence'] = doc.text

        entity_df = entity_df.append(df_row, ignore_index=True)

    # Put 'sentence' column first
    cols = sorted(list(entity_df.columns))
    cols.insert(0, cols.pop(cols.index('sentence')))
    entity_df = entity_df[cols]

    return entity_df
