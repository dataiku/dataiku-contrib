
# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *
import pandas as pd, numpy as np
from gensim.models import word2vec
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s',\
    level=logging.INFO )

input_text_dataset_name = get_input_names_for_role('input_text_dataset')[0] #.split(".")[1]
input_text_dataset = dataiku.Dataset(input_text_dataset_name) 

# For outputs, the process is the same:
model_repository = dataiku.Folder(get_output_names_for_role('model_repository')[0])
model_repository_path = model_repository.get_path()

config = get_recipe_config()

text_column = config['text_column']
sg = config.get('sg')
if sg == 'cbow' :
	sg = 0 
else :
	sg = 1	
size = int(config['size'])
iterations = int(config['iter'])
workers = int(config['workers'])
min_count = int(config['min_count'])    
max_vocab_size = int(config.get('max_vocab_size', None))    
window = int(config['window'])                
sample = config['sample']
negative = int(config['negative'])             
hs = int(config.get('hs', 0)) 
alpha = config['alpha']
cbow_mean = int(config.get('cbow_mean',0)) 
seed = int(config['seed'])             

modelname = "word2vec_model"


# here I could change the iterator to make it work also on docs. 
# define iterator on the dataset
class html_struct_corpus(object):
    def __init__(self, dataset,column_name):
        self.dataset = dataset
        self.column_name = column_name
                
    def __iter__(self):
        for row in self.dataset.iter_rows(): 
            yield row[self.column_name].split()

print "Training model..."

mycorpus = html_struct_corpus(input_text_dataset,text_column)

model = word2vec.Word2Vec(sentences=mycorpus \
			, size=size, alpha=alpha, window=window \
			, min_count=min_count, max_vocab_size=max_vocab_size \
			, sample=sample, seed=seed, workers=workers \
			, sg=sg, hs=hs, negative=negative, cbow_mean=cbow_mean \
			, iter=iterations)

# calling init_sims will make the model much more memory-efficient.
model.init_sims(replace=True)

model.save(model_repository_path + '/' + modelname)
