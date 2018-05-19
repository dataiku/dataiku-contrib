# -*- coding: utf-8 -*-
import os
from itertools import izip
import logging

import pandas as pd, numpy as np

from sklearn.metrics import pairwise_distances
from sklearn.linear_model import Ridge, lars_path
from sklearn.utils import check_random_state

from .preprocessing import LimePreprocessor

logger = logging.getLogger(__name__)

def exlpain_instance(*args, **kwargs):
    """
    Wrapper for webapp backend - Expains a single instance
    """
    raise NotImplementedError()

class LimeKernel(object):
    
    kernel_width=None
    
    def __init__(self, kernel_width):
        self.kernel_width = kernel_width
        
    def kernel_fn(self,d): 
        return np.sqrt(np.exp(-(d**2) / self.kernel_width ** 2))
    
    def transform(self, X):
        #TODO: check copy()
        return self.kernel_fn(X)
            
class LimeExplainer(object):
    """
    LimeExplainer works on tabular data in two modes:
    - on numpy arrays from a LimePreprocessor
    - directly on dataframes, in that case it will use its internal preprocessor
    """
    preprocessor = None
    kernel = None
    distance = None
    interpolation_data = None
    random_state = None
    ridge_alpha = None
    
    def __init__(self, train_df, saved_model, kernel_width, ridge_alpha=float(1.0) , preprocessing_params=None, random_state=None):
        self.random_state = check_random_state(random_state)
        self.preprocessor = LimePreprocessor(saved_model)
        self.kernel = LimeKernel(kernel_width)
        self.preprocessor.fit(train_df)
        #used for rigde regression
        self.ridge_alpha = ridge_alpha

    def iter_explain(self, instances_df, nh_size):

        [Xs, Ys, isSparse] = self.preprocessor.generate_samples(nh_size)
        [Xe, Ye, isSparse] = self.preprocessor.preprocess(instances_df)

        sample_weights = self.compute_sample_weights_to_instance(Xe, Xs)
        classes = self.preprocessor.get_classes()
        predictor_features = self.preprocessor.get_predictor_features()
        coefs_cols = ['coef_{}'.format(c) for c in classes]
        predictor_features_df = pd.DataFrame(predictor_features, columns=['feature'])
        samples_cols = ['sample_{}'.format(s) for s in range(nh_size)]

        for row_idx, [to_exp, to_proba, w] in enumerate(izip(Xe, Ye, sample_weights)):
            Xs[0,:] = to_exp
            Ys[0,:] = to_proba
            model_regressor = Ridge(alpha=self.ridge_alpha, fit_intercept=True, random_state=self.random_state)
            #TODO: compare with train explanation learning
            model_regressor.fit(Xs,Ys, sample_weight=w)
            local_r2_score = model_regressor.score(Xs, Ys, sample_weight=None)
            intercept_np = model_regressor.intercept_
            model_coefs = model_regressor.coef_
            kernel_distance_avg = np.mean(w)
            kernel_distance_std = np.std(w)

            coefs_df = pd.DataFrame(model_coefs.T, columns=coefs_cols)
            explanation_df = pd.concat((predictor_features_df,coefs_df), axis=1)
            #TODO: optimize this
            explanation_df.insert(0, '_exp_id', row_idx)

            instance_df = pd.DataFrame(to_exp.reshape(-1, len(to_exp)), columns=predictor_features)
            instance_df['r2_score'] = local_r2_score
            instance_df['kernel_distance_avg'] = kernel_distance_avg
            instance_df['kernel_distance_std'] = kernel_distance_std
            #TODO: optimize this
            instance_df.insert(0, '_exp_id', row_idx)

            #FIXME: used only for debugging 
            #weights_df = pd.DataFrame(w.reshape(-1, len(w)), columns=samples_cols)
            #weights_df.insert(0, '_exp_id', row_idx)

            yield explanation_df, instance_df


    def compute_distances_to_neighborhood(self, to_explain_X, samples_X):
        distances = pairwise_distances(to_explain_X, samples_X, metric='euclidean')
        #distance to self will always appear in first column
        distances[:,0] = np.zeros(distances.shape[0])
        return distances

    def compute_sample_weights_to_instance(self, to_explain_X, samples_X):
        distances = self.compute_distances_to_neighborhood(to_explain_X, samples_X)
        weights = self.kernel.transform(distances)
        return weights
    
