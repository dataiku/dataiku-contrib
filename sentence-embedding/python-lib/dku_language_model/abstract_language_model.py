# -*- coding: utf-8 -*-
import logging
logger = logging.getLogger(__name__)


class AbstractLanguageModel(object):
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = None
    
    @staticmethod
    def get_model_name():
        raise NotImplementedError()
        
    def load_model(self):
        raise NotImplementedError()
        
    def get_sentence_embedding(self, texts):
        raise NotImplementedError()

    def get_weighted_sentence_embedding(self, texts, smoothing_parameter, npc):
        return NotImplementedError()