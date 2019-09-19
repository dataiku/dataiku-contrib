# -*- coding: utf-8 -*-
import numpy as np
from collections import Counter
from gensim.models import KeyedVectors
from sklearn.decomposition import TruncatedSVD
from abstract_language_model import AbstractLanguageModel
from language_model_utils import clean_text
import logging
logger = logging.getLogger(__name__)


class ContextIndependentLanguageModel(AbstractLanguageModel):
      
    def __init__(self, model_path):
        AbstractLanguageModel.__init__(self, model_path)
        self.word2idx = None
        self.embedding_matrix = None

    def get_sentence_word_vectors(self, text):
        indices = [self.word2idx[w] for w in text.split() if w in self.word2idx]
        return self.embedding_matrix[indices]

    def compute_average_embedding(self, text):
        embeddings = self.get_sentence_word_vectors(text)
        avg_embedding = np.mean(embeddings, axis=0)
        return avg_embedding
        
    def get_sentence_embedding(self, texts):
        cleaned_texts = map(clean_text, texts)
        embeddings = map(self.compute_average_embedding, cleaned_texts)
        return embeddings.tolist()
        
    def get_weighted_sentence_word_vectors(self, text, weights):
        #Check if sentence contains at least one token and return None if not
        indices = [self.word2idx[w] for w in text.split() if w in self.word2idx]
        embeddings = self.embedding_matrix[indices]
        weights = [weights[w] for w in text.split() if w in self.word2idx]
        return [w * e for w, e in zip(weights, embeddings)]
    
    def compute_weighted_average_embedding(self, text, weights):
        """Weighted average embedding for computing SIF."""
        embeddings = self.get_weighted_sentence_word_vectors(text, weights)    
        avg_embedding = np.mean(embeddings, axis=0)
        return avg_embedding.tolist()
    
    def remove_principal_components(self, X, npc):
        """Removes the first PC for computing SIF."""
        svd = TruncatedSVD(n_components=npc, n_iter=7, random_state=0)
        X = np.array(X)
        svd.fit(X)
        u = svd.components_
        return X - X.dot(u.T).dot(u)

    def contruct_final_embeddings(self, embeddings, is_void):
        final_embeddings = []
        j = 0
        for v in is_void:
            if v == 0:
                final_embeddings.append(embeddings[j])
                j+=1
            else:
                final_embeddings.append(np.nan)
        return final_embeddings

    def get_weighted_sentence_embedding(self, texts, smoothing_parameter, npc):
        cleaned_texts = map(clean_text, texts)

        # Compute word weights
        word_weights = Counter()
        for text in cleaned_texts:
            word_weights.update(text.split())

        n_words = float(sum(word_weights.values()))
        for k, v in word_weights.items():
            word_weights[k] /= n_words
            word_weights[k] = smoothing_parameter / (smoothing_parameter + word_weights[k])

        # Compute SIF
        logger.info("Computing weighted average embeddings...")
        raw_embeddings = np.array(map(lambda s: self.compute_weighted_average_embedding(s, word_weights), cleaned_texts))

        # Remove empty sentences and save their indecies
        is_void = np.array(map(lambda x: x.shape == (), raw_embeddings))
        embeddings_without_void = [x for x,y in zip(raw_embeddings,is_void) if y==0]

        logger.info("Removing vectors principal component...")
        embeddings_processed = self.remove_principal_components(embeddings_without_void, npc)
        #TODO: refactor this to avoid the weird j index condition 
        final_embeddings = self.contruct_final_embeddings(embeddings_processed, is_void)
            
        return final_embeddings
    
    
class Word2vecModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'Word2Vec pretrained model'
    
    def load_model(self):
        logger.info('Loading Word2Vec model...')
        model = KeyedVectors.load_word2vec_format(self.model_path, binary=True)
        logger.info('Done')
        self.word2idx = {w: i for i, w in enumerate(model.index2word)}
        self.embedding_matrix = model.vectors


class FasttextModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'fastText pretrained model'
        
    def load_model(self):
        logger.info('Loading fastText model...')
        word2idx = {}
        embedding_matrix = []
        with open(self.model_path, 'r') as f:
            for i, line in enumerate(f):
                if i == 0:
                    continue
                if i != 0 and i % 100000 == 0:
                    logger.info("Loaded {} word embeddings".format(i))
                split = line.strip().split(' ')
                word, vector = split[0], split[1:]
                word2idx[word] = i

                embedding = np.array(vector).astype(float)
                embedding_matrix.append(embedding)
        logger.info('Done')
        self.embedding_matrix = np.array(embedding_matrix)
        self.word2idx = word2idx


class GloveModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'GloVe pretrained model.'
        
    def load_model(self):
        logger.info('Loading GloVe model...')
        word2idx = {}
        embedding_matrix = []
        with open(self.model_path, 'r') as f:
            for i, line in enumerate(f):
                if i != 0 and i % 100000 == 0:
                    logger.info("Loaded {} word embeddings".format(i))
                split = line.strip().split(' ')
                word, vector = split[0], split[1:]
                word2idx[word] = i
                embedding = np.array(vector).astype(float)
                embedding_matrix.append(embedding)
            logger.info("Successfully loaded {} word embeddings!".format(i))
        self.embedding_matrix = np.array(embedding_matrix)
        self.word2idx = word2idx


class CustomModel(ContextIndependentLanguageModel):
    
    @staticmethod
    def get_model_name():
        return 'Custom pretrained model.'
            
    def load_model(self):
        logger.info('Loading custom model...')
        word2idx = {}
        embedding_matrix = []
        with open(self.model_path, 'r') as f:
            for i, line in enumerate(f):
                if i != 0 and i % 100000 == 0:
                    logger.info("Loaded {} word embeddings".format(i))
                split = line.strip().split(' ')
                word, vector = split[0], split[1:]
                word2idx[word] = i
                embedding = np.array(vector).astype(float)
                embedding_matrix.append(embedding)
            logger.info("Successfully loaded {} word embeddings!".format(i))
        self.embedding_matrix = np.array(embedding_matrix)
        self.word2idx = word2idx