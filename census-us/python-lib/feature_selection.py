# -*- coding: utf-8 -*-
import dataiku
from sklearn.feature_selection import SelectKBest,SelectPercentile, f_classif,f_regression,chi2
import numpy as np
from dataiku.customrecipe import *

def univariate_feature_selection(mode,predictors,target):
    
    if mode == 'f_regression':
        fselect = SelectPercentile(f_regression, 100)
        
    if mode == 'f_classif':
        fselect = SelectPercentile(f_classif, 100)
        
    if mode == 'chi2':
        fselect = SelectPercentile(chi2, 100)
        
    fselect.fit_transform(predictors, target)
    
    return fselect.pvalues_