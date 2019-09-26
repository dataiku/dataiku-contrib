# -*- coding: utf-8 -*-
import os
import logging
from dku_language_model.context_independent_language_model import FasttextModel, Word2vecModel, GloveModel, CustomModel
from dku_language_model.contextual_language_model import ElmoModel

import string
maketrans = string.maketrans

###########################################################################
# LOGGING CONFIG
###########################################################################

FORMAT = '[SENTENCE EMBEDDING] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_pretrained_model(path, embedding_is_custom=False):
    folder_contents = os.listdir(path)
    if len(folder_contents) == 0:
        raise Exception("Pre-trained embeddings folder is empty!")
    elif len(folder_contents) > 1:
        raise Exception("Too many files in the pre-trained embeddings folder. " +
                        "It should only contain one file, or in the case of ELMO a folder.")

    embedding_file = folder_contents[0]
    embedding_file_path = os.path.join(path, embedding_file)
    if embedding_is_custom:
        try:
            with open(embedding_file_path, 'r') as f:
                pass
        except IOError:
            raise Exception('Custom embedding should be a readable .txt file.')
        model = CustomModel(embedding_file_path)
    else:
        if embedding_file == "fastText_embeddings":
            model = FasttextModel(embedding_file_path)
        elif embedding_file == "GloVe_embeddings":
            model = GloveModel(embedding_file_path)
        elif embedding_file == "Word2vec_embeddings":
            model = Word2vecModel(embedding_file_path)
        elif embedding_file == "ELMo":
            model = ElmoModel(embedding_file_path)
        else:
            raise ValueError("Something is wrong with the pre-trained embeddings. " +
                             "Please make sure to either use the plugin macro to download the embeddings, " +
                             "or tick the custom embedding box if you are using custom vectors.")

    model.load_model()
    return model