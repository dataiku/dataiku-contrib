# -*- coding: utf-8 -*-
import os
import re
import json
import logging
import numpy as np
import dataiku

from collections import Counter, defaultdict
from sklearn.decomposition import TruncatedSVD
from gensim.models import KeyedVectors
from dataiku.customrecipe import *

import string
maketrans = string.maketrans


###########################################################################
# LOGGING CONFIG
###########################################################################

FORMAT = '[SENTENCE EMBEDDING] %(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


###########################################################################
# PRE-PROCESSING
###########################################################################

# Twitter related tokens
RE_HASHTAG = ur'#[a-zA-Z0-9_]+'
RE_MENTION = ur'@[a-zA-Z0-9_]+'

RE_URL = ur'(?:https?://|www\.)(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
RE_EMAIL = ur'\b[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+\b'

TOKENS_TO_IGNORE = [
    RE_HASHTAG,
    RE_MENTION,
    RE_URL,
    RE_EMAIL
]


def clean_text(text):
    """
    Applies some pre-processing to clean text data.

    In particular:
    - lowers the string
    - removes URLs, e-mail adresses
    - removes Twitter mentions and hastags
    - removes HTML tags
    - removes the character [']
    - replaces punctuation with spaces

    """

    text = text.lower()  # lower text

    # ignore urls, mails, twitter mentions and hashtags
    for regex in TOKENS_TO_IGNORE:
        text = re.sub(regex, ' ', text)

    text = re.sub(r'<[^>]*>', ' ', text)  # remove HTML tags if any

    # remove the character [']
    text = re.sub(r"\'", "", text)

    # this is the default cleaning in Keras,
    # it consists in lowering the texts and removing the punctuation
    filters = '!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n'

    split = " "  # character that will be used to split the texts later

    if isinstance(text, unicode):
        translate_map = dict((ord(c), unicode(split)) for c in filters)
        text = text.translate(translate_map)
    elif len(split) == 1:
        translate_map = maketrans(filters, split * len(filters))
        text = text.translate(translate_map)
    else:
        for c in filters:
            text = text.replace(c, split)

    return text


###########################################################################
# WORD EMBEDDING LOADERS
###########################################################################

class EmbeddingModel():
    """
    A class for loading pre-trained word embeddings.
    """

    def __init__(self, path, embedding_is_custom):
        self.guess_origin_from_folder_contents(path, embedding_is_custom)
        self.load_embeddings()

    def guess_origin_from_folder_contents(self, path, embedding_is_custom):

        folder_contents = os.listdir(path)
        if len(folder_contents) == 0:
            raise Exception("Pre-trained embeddings folder is empty!")
        elif len(folder_contents) > 1:
            raise Exception("Too many files in the pre-trained embeddings folder. " +
                            "It should only contain one file, or in the case of ELMO a folder.")            
            
        embedding_file = folder_contents[0]
        self.embedding_file_path = os.path.join(path, embedding_file)
        if embedding_is_custom:
            try:
                with open(self.embedding_file_path, 'r') as f:
                    pass
            except IOError:
                raise Exception('Custom embedding should be a readable .txt file.')
            
            self.origin = "custom"
        else:
            if embedding_file == "fastText_embeddings":
                self.origin = "fasttext"
            elif embedding_file == "GloVe_embeddings":
                self.origin = "glove"
            elif embedding_file == "Word2vec_embeddings":
                self.origin = "word2vec"
            elif embedding_file == "ELMo":
                self.origin = "elmo"
            else:
                raise ValueError("Something is wrong with the pre-trained embeddings. " +
                                 "Please make sure to either use the plugin macro to download the embeddings, " +
                                 "or tick the custom embedding box if you are using custom vectors.")

    def load_embeddings(self):

        if self.origin == "word2vec":

            #######################
            # Word2vec
            #######################

            logger.info("Loading Word2vec embeddings...")
            model = KeyedVectors.load_word2vec_format(
                self.embedding_file_path, binary=True)

            self.word2idx = {w: i for i, w in enumerate(model.index2word)}
            self.embedding_matrix = model.vectors

        elif self.origin in ["custom", "glove", "fasttext"]:

            #######################################
            # GloVe, FastText or Custom Embedding
            #######################################

            word2idx = {}
            embedding_matrix = []
            with open(self.embedding_file_path, 'r') as f:
                for i, line in enumerate(f):

                    if i == 0 and self.origin == "fasttext":
                        continue

                    if i != 0 and i % 100000 == 0:
                        logger.info("Loaded {} word embeddings".format(i))

                    split = line.strip().split(' ')
                    word, vector = split[0], split[1:]

                    word2idx[word] = i

                    embedding = np.array(vector).astype(float)
                    embedding_matrix.append(embedding)

                logger.info(
                    "Successfully loaded {} word embeddings!".format(i))

            self.embedding_matrix = np.array(embedding_matrix)
            self.word2idx = word2idx

        elif self.origin == "elmo":

            #######################
            # ELMo
            #######################

            # Set path for loading the pre-trained ELMo model
            os.environ["TFHUB_CACHE_DIR"] = os.path.join(
                self.embedding_file_path)

            # Avoid importing TensorFlow if not using ELMo to avoid TF-related errors
            import tensorflow as tf
            import tensorflow_hub as hub

            logger.info("Initializing ELMo...")
            self.elmo = hub.Module(
                "https://tfhub.dev/google/elmo/2", trainable=False)

            # Initialize the model
            self.sess = tf.Session()
            self.sess.run(tf.global_variables_initializer())
            self.sess.run(tf.tables_initializer())

        else:
            raise Exception("Something is wrong with the embedding origin.")

    def get_sentence_word_vectors(self, sentence):

        if self.origin == "elmo":
            raise NotImplementedError

        indices = [self.word2idx[w]
                   for w in sentence.split() if w in self.word2idx]
        return self.embedding_matrix[indices]

    def get_weighted_sentence_word_vectors(self, sentence, weights):

        if self.origin == "elmo":
            raise NotImplementedError

        indices = [self.word2idx[w]
                   for w in sentence.split() if w in self.word2idx]
        embeddings = self.embedding_matrix[indices]
        weights = [weights[w] for w in sentence.split() if w in self.word2idx]
        return [w * e for w, e in zip(weights, embeddings)]


###########################################################################
# SENTENCE EMBEDDING COMPUTATION
###########################################################################

def preprocess_and_compute_sentence_embedding(texts, embedding_model, method, smoothing_parameter, npc):
    """
    Takes a dataframe, a column name, and embedding model and an aggregation
    method then returns a sentence embeddings using the chosen method.
    """

    def elmo_batch_average(texts):
        max_sequence_length = 100
        batch_size = 32

        logger.info("Creating text batches for ELMo")
        texts = [' '.join(s.split()[:max_sequence_length]) for s in texts]
        n_texts = len(texts)

        text_batches = (
            [texts[i * batch_size: (i + 1) * batch_size]
             for i in range(n_texts // batch_size)]
            + [texts[(n_texts // batch_size)*batch_size:]])

        logger.info("Computing sentence embeddings using ELMo...")
        text_embeddings = []
        for batch in text_batches:
            tensors = embedding_model.elmo(
                batch, signature="default", as_dict=True)["default"]
            embeddings = embedding_model.sess.run(tensors)
            text_embeddings.extend(embeddings.tolist())
            logger.info("Done {} over {} texts...".format(
                len(text_embeddings), n_texts))

        return text_embeddings

    def average_embedding(text):
        """Get average word embedding from models like Word2vec, Glove or FastText."""
        embeddings = embedding_model.get_sentence_word_vectors(text)
        avg_embedding = np.mean(embeddings, axis=0)
        return avg_embedding

    def weighted_average_embedding(text, weights):
        """Weighted average embedding for computing SIF."""
        embeddings = embedding_model.get_weighted_sentence_word_vectors(
            text, weights)
        avg_embedding = np.mean(embeddings, axis=0)
        return avg_embedding

    def remove_first_principal_component(X):
        """Removes the first PC for computing SIF."""
        svd = TruncatedSVD(n_components=npc, n_iter=7, random_state=0)
        X = np.array(X)
        svd.fit(X)
        u = svd.components_
        return X - X.dot(u.T).dot(u)

    #####################################################

    logger.info("Pre-processing texts...")
    clean_texts = map(clean_text, texts)

    # Computing either simple average or weighted average embedding
    method_name = method + "_" + embedding_model.origin

    if method == 'simple_average':

        if embedding_model.origin != "elmo":
            logger.info("Computing simple average embeddings...")
            res = map(average_embedding, clean_texts)
        else:
            res = elmo_batch_average(clean_texts)

    elif method == 'SIF':

        if embedding_model.origin != "elmo":

            # Compute word weights
            word_weights = Counter()
            for s in clean_texts:
                word_weights.update(s.split())

            n_words = float(sum(word_weights.values()))
            for k, v in word_weights.items():
                word_weights[k] /= n_words
                word_weights[k] = float(
                    smoothing_parameter) / (smoothing_parameter + word_weights[k])

            # Compute SIF
            logger.info("Computing weighted average embeddings...")
            res = map(lambda s: weighted_average_embedding(
                s, word_weights), clean_texts)

            logger.info("Removing vectors first principal component...")
            res = remove_first_principal_component(res)
        else:
            raise NotImplementedError(
                "SIF is not implemented for ELMo. Please choose 'Simple Average' instead.")

    else:
        raise NotImplementedError(
            "Only available aggregation methods are: 'simple_average' and 'SIF'.")

    return res