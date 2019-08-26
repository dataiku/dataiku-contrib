# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import numpy as np
import re
import string
from collections import Counter, defaultdict
from gensim.models import KeyedVectors
from sklearn.decomposition import TruncatedSVD
import tensorflow as tf
import tensorflow_hub as hub
import logging
logger = logging.getLogger(__name__)


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

#Counter to monitor ELMO embedding comuputations
COUNT = 0

class ContextualLanguageModel(object):
      
    def __init__(self, model_path):
        self.model_path = model_path
        self.sess = None
        self.model = None

        
    @staticmethod
    def get_model_name():
        raise NotImplementedError()
        
    @staticmethod
    def download_model():
        raise NotImplementedError()
        
    def load_model(self):
        raise NotImplementedError()
        
    def get_embedding(self, text):
        raise NotImplementedError()
        
    def finetuning(self):
        raise NotImplementedError()
        
    def get_sentence_word_vectors(self, text):
        return NotImplementedError()
        
    def get_sentence_embedding(self, batch):
        cleaned_batch = map(self.clean_text, batch)
        res = map(self.compute_average_embedding, cleaned_batch)
        return res
    
    @staticmethod
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

        text = str(text).lower()  # lower text

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
            translate_map = string.maketrans(filters, split * len(filters))
            text = text.translate(translate_map)
        else:
            for c in filters:
                text = text.replace(c, split)
        return text
    
    
class ElmoModel(ContextualLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'ELMO pretrained model'
    
    def load_model(self):
        # Set path for loading the pre-trained ELMo model
        os.environ["TFHUB_CACHE_DIR"] = os.path.join(self.model_path)
        print("Initializing ELMo...")
        self.model = hub.Module("https://tfhub.dev/google/elmo/2", trainable=True)
        # Initialize the model
        self.sess = tf.Session()
        self.sess.run(tf.global_variables_initializer())
        self.sess.run(tf.tables_initializer())
        
        
    def get_text_batches(self, texts):
        max_sequence_length = 100
        batch_size = 32

        logger.info("Creating text batches for ELMo")
        texts = [' '.join(s.split()[:max_sequence_length]) for s in texts]
        n_texts = len(texts)

        text_batches = ([texts[i * batch_size: (i + 1) * batch_size] for i in range(n_texts // batch_size)]
                        + [texts[(n_texts // batch_size)*batch_size:]])
        return text_batches
    
    def get_sentence_embedding(self, texts):
        cleaned_texts = map(self.clean_text, texts)
        batches = self.get_text_batches(cleaned_texts)
        embedded_sentences = []
        for batch in batches:
            tensors = self.model(batch, signature="default", as_dict=True)["default"]
            embeddings = self.sess.run(tensors)
            embedded_sentences.extend(embeddings.tolist())
        return embedded_sentences