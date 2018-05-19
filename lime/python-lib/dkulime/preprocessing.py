# -*- coding: utf-8 -*-
import os
import logging

import pandas as pd, numpy as np

from sklearn.preprocessing import LabelEncoder
from sklearn.utils import check_random_state
from sklearn.linear_model import Ridge, lars_path
from sklearn.metrics import pairwise_distances

from dataiku.doctor import constants
from dataiku.core.intercom import backend_json_call, backend_void_call

from .data_collection import LimeTrainDataAnalysis

logger = logging.getLogger(__name__)

class LimePreprocessor(object):
    
    random_state = None
    predictor = None

    #type: dataiku.core.saved_model.ModelParams
    #class ModelParams:
    #  def __init__(self, model_type, modeling_params, preprocessing_params, core_params, schema, user_meta, model_perf,
    #             target_map,
    #             conditional_outputs, cluster_name_map):
    #      self.modeling_params = modeling_params
    #      self.preprocessing_params = preprocessing_params
    #      self.core_params = core_params
    #      self.user_meta = user_meta
    #      self.schema = schema
    #      self.model_perf = model_perf
    #      self.model_type = model_type
    #      self.target_map = {t["mappedValue"]: t["sourceValue"] for t in preprocessing_params["target_remapping"]}
    #      self.conditional_outputs = conditional_outputs
    #      self.cluster_name_map = cluster_name_map
    predictor_params = None
    predictor_features = None
    classes = None
    predictor_proba_fmt = None

    #Original collector data from saved model preprocessor
    #TODO: should be used to benchmark preprocessing - to be updated at fit time too ?
    collector_data = None
    #Analysis data of the train set according to LIME
    train_analysis_data = None

    def __init__(self, saved_model, project_key=None, random_state=None):
        
        self.encoder = None
        self.categorical_names_map = None
        self.classes = None

        self.random_state = check_random_state(random_state)

        if project_key:
            self.project_key = project_key
        else:
            try:
                self.project_key = os.environ["DKU_CURRENT_PROJECT_KEY"]
            except:
                raise Exception('you must provide a project key or run the lib from DSS')

        try:
            self.predictor = saved_model.get_predictor()
            self.predictor_params = self.predictor.params
            self.predictor_features = self.predictor.get_features()
        except:
            raise

        #Sanity check
        if self.predictor.params.model_type != "PREDICTION":
            raise TypeError('Lime Preprocessor applies only to prediction models')
        else:
            if self.predictor.params.core_params[constants.PREDICTION_TYPE] == 'REGRESSION':
                #TODO implement regression
                raise NotImplementedError('Lime Preprocessor does not implement Regression')
                
        self.classes = self.get_classes()
        #additional sanity check for multi-class
        if self.classes is None:
            raise ValueError('Predictor does not seem to be a classifier, no classes found')
        
        #FIXME: hardcoded - anyway to retreive this dynamically?
        self.predictor_proba_fmt = 'proba_{}'
        

    def get_predictor_params(self):
        return self.predictor.params

    def get_predictor_features(self):
        return self.predictor_features

    def get_predictor_params_list(self):
        return [ (attr, getattr(self.predictor.params, attr)) for attr in dir(self.predictor.params) if not callable(getattr(self.predictor.params, attr))]

    def get_modeling_params(self):
        return self.predictor.preprocessing.modeling_params


    def get_target(self):
        return self.predictor_params.core_params.get(constants.TARGET_VARIABLE, None)

    def is_fittable(self, df):
        #TODO: impleent
        logger.warn('is_fittable is not implemented')
        return True

    def fit(self, df):
        #TODO: implement
        if self.is_fittable(df): 
            collector = LimeTrainDataAnalysis(df, self.predictor.params.preprocessing_params)
            self.train_analysis_data = collector.build()
        else:
            raise AttributeError('Dataset is not fittable')

        return self
        
    def get_classes(self):
         return self.predictor.get_classes()

    def get_features(self):
        """ returns features list """
        if self.train_analysis_data:
            try:
                return self.train_analysis_data['feature_order']
            except KeyError:
                raise Exception('feature_order does not exists in train_analysis_data %s' % self.train_analysis_data.keys())
            else:
                raise
        else:
            logger.warn('Lime preprocessor does not seem to be fitted, no train_analysis_data')
            return None

    def get_feature_stats(self, fname):
        try:
            analysis_per_feature = self.train_analysis_data['per_feature']
        except:
            raise Exception('Analysis data collector schema does not match')

        return analysis_per_feature.get(fname, None)

    def get_feature_config(self, fname):
        try:
            ppp_per_feature = self.predictor_params.preprocessing_params['per_feature']
        except:
            raise Exception('Pre-processing params schema does not match')

        return ppp_per_feature.get(fname, None)

    def get_feature_role(self, fname):
        return self.get_feature_config(fname).get('role', None)

    def get_feature_type(self, fname):
        return self.get_feature_config(fname).get('type', None)

    def generate_normal_np(self, num_samples):
        features_list = self.get_features()
        return self.random_state.normal(
                    0, 1, num_samples * len(features_list)).reshape(
                    num_samples, len(features_list))

    def generate_normal(self, num_samples):
        return pd.DataFrame(self.generate_normal_np(num_samples), columns=self.get_features())
    
    def generate_inverse_cat_feature(self, fname, num_samples):
        f_stats = self.get_feature_stats(fname)
        if f_stats is None:
            raise ValueError('Feature %s not in model input feature list' % fname)
        
        total_count = f_stats['total_count']
        values, freqs = map(list,  zip(*[(k, float(v) / float(total_count) ) for k,v in f_stats['category_value_count']]))
        inverse_column = self.random_state.choice(values, size=num_samples,
                                                      replace=True, p=freqs)
        return inverse_column
    
    def generate_inverse_num_feature(self, fname, num_samples):
        f_stats = self.get_feature_stats(fname)
        if f_stats is None:
            raise ValueError('Feature %s not in model input feature list' % fname)
        
        _mean = f_stats['stats']['average']
        _std = f_stats['stats']['std']
        _nulls = f_stats['nulls_count']
        inverse_col_np = self.random_state.normal(0, 1, num_samples)
        inverse_col_np = inverse_col_np * _std + _mean
        return inverse_col_np

    def generate_inverse(self, num_samples):
        inverse_df = self.generate_normal(num_samples)
        for f in inverse_df.columns.tolist():
            f_config = self.get_feature_config(f)
            if not f_config:
                logger.warn('Feature %s does not exist in pre-processing params, skipping inversion' % f)
                continue

            f_role = self.get_feature_role(f)
            f_type = self.get_feature_type(f)

            if f_role != 'INPUT':
                logger.warn('Skipping feature %s because not an INPUT feature, its role is %s' % (f, f_role))
                continue
                
            if f_type == 'NUMERIC':
                inverse_df[f] = self.generate_inverse_num_feature(f, num_samples)
            elif f_type == 'CATEGORY':
                inverse_df[f] = self.generate_inverse_cat_feature(f, num_samples)
            else:
                raise ValueError('Unknown feature type %s for feature %s' % (f_type, f))
                
        return inverse_df

    def generate_samples(self, num_samples):
        from dataiku.doctor.prediction import prepare_multiframe
        inverse_df = self.generate_inverse(num_samples)
        inverse_mf = self.predictor.preprocessing.pipeline.process(inverse_df)
        X,isSparse = prepare_multiframe(inverse_mf['TRAIN'], self.predictor.preprocessing.modeling_params)
        Y = self.predictor._clf.predict_proba(X)
        return X, Y, isSparse
    
    def generate_samples_to_df(self, num_samples):
        X,Y, isSparse = self.generate_samples(num_samples)
        #we want this to fail if classes are None
        Y_df = pd.DataFrame(Y, columns=[self.predictor_proba_fmt.format(c) for c in self.classes])
        X_df = pd.DataFrame(X, columns=self.predictor_features)
        return pd.concat((X_df, Y_df), axis=1)

    def preprocess(self, test_df):
        test_mf = self.predictor.preprocessing.pipeline.process(test_df)
        #Forcing np_array output
        #prediction preprocessors return one key dataframe 'TRAIN' less complex than clustering
        #see preprocessing_handler.py
        #TODO: implement finer algorithm selection (from INGRID dict?)
        X, isSparse = test_mf['TRAIN'].as_np_array(), False
        Y = self.predictor._clf.predict_proba(X)
        return X, Y, isSparse

    @property
    def dtypes(self):
        return self._dtypes
    
    #TODO: add metrics to return
    @classmethod
    def encode_categorical_features(cls, df):
        cat_feature_map = OrderedDict()
        for pos, f in enumerate(df):
            if not np.issubdtype(df[f].dtype, np.number):
                encoder = LabelEncoder()
                df[f] = encoder.fit_transform(df[f])
                #TODO: must ensure the mapping is consistent
                cat_feature_map[pos] = encoder.classes_.tolist()

        return cat_feature_map
    
    @classmethod
    def decode_categorical_features(cls, df, cat_feature_map):
        columns = df.columns.tolist()
        for pos in cat_feature_map.keys():
            df[columns[pos]] = df[columns[pos]].apply(lambda x: cat_feature_map[pos][int(x)])
        
        return df

    def get_model_folder(self, version_id=None):
        
        if version_id is None:
            version_id = [x for x in self.saved_model.list_versions() if x["active"]][0]["versionId"]
            
        res = backend_json_call("savedmodels/get-model-details", data={
            "projectKey": self.project_key,
            "smId": self.saved_model.get_id(),
            "versionId": version_id
        })
        
        model_folder = res["model_folder"]

        return model_folder
            
