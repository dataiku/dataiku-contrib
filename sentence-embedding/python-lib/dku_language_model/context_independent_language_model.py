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

class ContextIndependentLanguageModel(object):
      
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None
        self.word2idx = None
        self.embedding_matrix = None
        
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
        indices = [self.word2idx[w] for w in text.split() if w in self.word2idx]
        return self.embedding_matrix[indices]

    def compute_average_embedding(self, text):
        embeddings = self.get_sentence_word_vectors(text)
        avg_embedding = np.mean(embeddings, axis=0)
        return avg_embedding
        
    def get_sentence_embedding(self, texts):
        cleaned_texts = map(self.clean_text, texts)
        res = map(self.compute_average_embedding, texts)
        return res
        
    def get_weighted_sentence_word_vectors(self, batch, weights):
        #Check if sentence contains at least one token and return None if not
        indices = [self.word2idx[w]for w in batch.split() if w in self.word2idx]
        embeddings = self.embedding_matrix[indices]
        weights = [weights[w] for w in batch.split() if w in self.word2idx]
        return [w * e for w, e in zip(weights, embeddings)]
    
    def compute_weighted_average_embedding(self, text, weights):
        """Weighted average embedding for computing SIF."""
        embeddings = self.get_weighted_sentence_word_vectors(text, weights)    
        avg_embedding = np.mean(embeddings, axis=0)
        return avg_embedding
    
    def remove_first_principal_component(self, X, npc):
        """Removes the first PC for computing SIF."""
        svd = TruncatedSVD(n_components=npc, n_iter=7, random_state=0)
        X = np.array(X)
        svd.fit(X)
        u = svd.components_
        return X - X.dot(u.T).dot(u)

    def contruct_final_res(self, res,is_void):
        res_final = []
        j = 0
        for v in is_void:
            if v == 0:
                res_final.append(res[j])
                j+=1
            else:
                res_final.append(np.nan)
        return res_final
    
    
    def get_weighted_sentence_embedding(self, texts, smoothing_parameter, npc):
        cleaned_texts = map(self.clean_text, texts)

        # Compute word weights
        word_weights = Counter()
        for text in cleaned_texts:
            word_weights.update(text.split())

        n_words = float(sum(word_weights.values()))
        for k, v in word_weights.items():
            word_weights[k] /= n_words
            word_weights[k] = float(smoothing_parameter) / (smoothing_parameter + word_weights[k])

        # Compute SIF
        logger.info("Computing weighted average embeddings...")
        res = map(lambda s: self.compute_weighted_average_embedding(s, word_weights), cleaned_texts)

        #Remove empty sentences and save their indecies
        is_void = map(lambda x: 1 if x.shape == () else 0 , res)
        res = [x for x,y in zip(res,is_void) if y==0]

        logger.info("Removing vectors first principal component...")
        res = self.remove_first_principal_component(res, npc)
        res = self.contruct_final_res(res,is_void)
            
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
    
    
class Word2vecModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'Word2Vec pretrained model'
    
    def load_model(self):
        print('Loading Word2Vec model...')
        model = KeyedVectors.load_word2vec_format(self.model_path, binary=True)
        print('Done.')
        self.word2idx = {w: i for i, w in enumerate(model.index2word)}
        self.embedding_matrix = model.vectors

class FasttextModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'fastText pretrained model'
        
    def load_model(self):
        print('Loading fastText model...')
        word2idx = {}
        embedding_matrix = []
        with open(self.model_path, 'r') as f:
            for i, line in enumerate(f):
                if i == 0:
                    continue
                if i != 0 and i % 100000 == 0:
                    print("Loaded {} word embeddings".format(i))
                split = line.strip().split(' ')
                word, vector = split[0], split[1:]
                word2idx[word] = i

                embedding = np.array(vector).astype(float)
                embedding_matrix.append(embedding)
        print('Done.')
        self.embedding_matrix = np.array(embedding_matrix)
        self.word2idx = word2idx
    
class GloveModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'GloVe pretrained model.'
        
    def load_model(self):
        print('Loading GloVe model...')
        word2idx = {}
        embedding_matrix = []
        with open(self.model_path, 'r') as f:
            for i, line in enumerate(f):
                if i != 0 and i % 100000 == 0:
                    print("Loaded {} word embeddings".format(i))
                split = line.strip().split(' ')
                word, vector = split[0], split[1:]
                word2idx[word] = i
                embedding = np.array(vector).astype(float)
                embedding_matrix.append(embedding)
            logger.info("Successfully loaded {} word embeddings!".format(i))
        print('Done')
        self.embedding_matrix = np.array(embedding_matrix)
        self.word2idx = word2idx
    
class CustomModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'Custom pretrained model.'
            
    def load_model(self):
        print('Loading custom model...')
        word2idx = {}
        embedding_matrix = []
        with open(self.model_path, 'r') as f:
            for i, line in enumerate(f):
                if i != 0 and i % 100000 == 0:
                    print("Loaded {} word embeddings".format(i))
                split = line.strip().split(' ')
                word, vector = split[0], split[1:]
                word2idx[word] = i
                embedding = np.array(vector).astype(float)
                embedding_matrix.append(embedding)
            logger.info("Successfully loaded {} word embeddings!".format(i))
        print('Done')
        self.embedding_matrix = np.array(embedding_matrix)
        self.word2idx = word2idx
        