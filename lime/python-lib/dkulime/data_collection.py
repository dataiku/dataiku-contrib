import numpy as np
import logging
import json
import math
from dataiku.doctor import constants
from dataiku.doctor.preprocessing_collector import PreprocessingDataCollector
from collections import Counter


logger = logging.getLogger(__name__)

NULL_CAT = 'NULL_Value'

class LimeTrainDataAnalysis(PreprocessingDataCollector):

    def __init__(self, train_df, preprocessing_params, max_cat_safety=200):
        PreprocessingDataCollector.__init__(self, train_df, preprocessing_params)
        self.max_cat_safety = max_cat_safety

    def feature_needs_analysis(self, params):
        #TODO: Skip WEIGHT and PROFILING for now
        return params["role"] in ("INPUT",)

    def get_feature_analysis_data(self, name, params):

        output = {"stats": {}}
        logger.info("Looking at %s... (type=%s)" % (name, params["type"]))
        series = self.df[name]

        if self.feature_needs_analysis(params):
            if params["type"] == 'NUMERIC':
                return self._get_numeric_feature_analysis_data(series, output)
            elif params["type"] == 'CATEGORY':
                return self._get_categorical_feature_analysis_data(series, output)
            elif params["type"] == "TEXT":
                return self._get_text_feature_analysis_data(series, output)
            elif params["type"] == "VECTOR":
                return self._get_vector_feature_analysis_data(series, output)
            else:
                return output

    def _get_numeric_feature_analysis_data(self, series, output):

	logger.info("Checking series of type: %s (isM8=%s)" % (series.dtype, series.dtype == np.dtype('M8[ns]')))

	if np.isinf(series).any():
	    raise ValueError("Numeric feature '%s' contains Infinity values" % name)

	output['stats'] = {
	    'min': series.min(),
	    'average': series.mean(),
	    'median': series.median(),
	    'max': series.max(),
	    'p99': series.quantile(0.99),
	    'std': series.std()
	}
        output['nulls_count'] = series.isnull().sum()

        return output

    def _get_categorical_feature_analysis_data(self, series, output):
        #FIXME: N/A will be generated as nan isinstance(float) np.isnan() true
        category_stats = series.value_counts(dropna=False)
        nulls = series.isnull().sum()
        candidates = [(k, v) for (k, v) in category_stats.iloc[0:self.max_cat_safety].iteritems()]
        output['category_value_count'] = candidates
        output['nulls_count'] = nulls
        output['total_count'] = sum(v for k,v in candidates)

        return output

    def _get_text_feature_analysis_data(self, series, output):
        logger.warn('TEXT feature not implemented, skipping analysis')
        return output
    def _get_vector_feature_analysis_data(self, series, output):
        logger.warn('VECTOR type feature analysis not implemented, skipping analysis')
        return output


