# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku.customrecipe import *
import gensim 

# Recipe inputs & outputs
input_text_dataset = dataiku.Dataset(get_input_names_for_role('input_text_dataset')[0])
model_repository = dataiku.Folder(get_input_names_for_role('model_repository')[0])
model_repository_path = model_repository.get_path()
output_text_dataset = dataiku.Dataset(get_output_names_for_role('output_text_dataset')[0])

# parameters
config = get_recipe_config()
column_name = config['text_column']
keep_all_cols = config['keep_all_cols']
agg_mean = config['agg_mean']
modelname = config["model_name"]
modelfile = model_repository_path + '/' + modelname

modelformat = config["model_format"]

def makeFeatureVec(words, model, num_features):
    featureVec = np.zeros((num_features,),dtype="float32")
    nwords = 0.
    for word in words:
        if word in index2word_set: 
            nwords = nwords + 1.
            featureVec = np.add(featureVec,model[word])
    if agg_mean : 
        featureVec = np.divide(featureVec,nwords)
    return featureVec

if modelformat == "gensim":
    model = gensim.models.Word2Vec.load(modelfile)
elif modelformat == "word2vec-text":
    model = gensim.models.Word2Vec.load_word2vec_format(modelfile, binary=False)
elif modelformat == "word2vec-binary":
    model = gensim.models.Word2Vec.load_word2vec_format(modelfile, binary=True)
else:
    raise Exception("Unknown model format: %s" % modelformat)
index2word_set = set(model.wv.vocab)
word2vecdim = model.wv.syn0.shape[1]

if keep_all_cols :
    myschema = [val for val in input_text_dataset.read_schema()]
else :
    myschema = []

for i in range(word2vecdim) : 
    myschema.append({"name": "word2vec_" + str(i),"type": "float"})
output_text_dataset.write_schema(myschema)
mywriter = output_text_dataset.get_writer()

for j, row in enumerate(input_text_dataset.iter_rows()) : 
    thedic = {}
    if keep_all_cols : 
        for key, val in row.items() :
                thedic[key] = val
    myvector = list(makeFeatureVec(row[column_name],model,word2vecdim))
    if not np.isnan(myvector[0]) : 
        for i in range(word2vecdim) : 
            thedic["word2vec_" + str(i)] = myvector[i]                
        mywriter.write_row_dict(thedic)
        
    if j % 1000 == 0 :
        print "done", j
       