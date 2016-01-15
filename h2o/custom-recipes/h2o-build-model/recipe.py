# coding: utf-8
import dataiku
from dataiku.customrecipe import *
import pandas as pd
import os, sys, json, shutil
import h2o
from h2o_utils import DSS_dataset_to_H2O_frame, saved_model_folder

## init h2o
ip = get_plugin_config()['h2o_server_ip']
port = int(get_plugin_config()['h2o_server_port'])
h2o.init(ip, port)

## create train_frame
factor_columns = get_recipe_config().get('factor_columns',[])
train_dataset = get_input_names_for_role('train_set')[0]
train_frame = DSS_dataset_to_H2O_frame(train_dataset)
for col in factor_columns:
    train_frame[col] = train_frame[col].asfactor()

## create valid_frame if ratio or validation set provided
valid_frame = None

train_ratio = get_recipe_config().get('train_ratio',-1.)
if train_ratio != -1. and train_ratio != 1. :
    old_train = train_frame
    train_frame, valid_frame = train_frame.split_frame(ratios=[train_ratio])
    print 'Split {} lines into {} for training and {} for validation'.format(len(old_train), len(train_frame), len(valid_frame))

valid_names = get_input_names_for_role('validation_set')
if valid_names:
    if train_ratio != -1. :
        raise Exception("You may specify either an input_dataset for validation, or a train ratio, but not both.")
    valid_frame = DSS_dataset_to_H2O_frame(valid_names[0])
    for col in get_recipe_config().get('factor_columns',[]):
        valid_frame[col] = valid_frame[col].asfactor()

## create target if needed
algorithm = get_recipe_config().get('algorithm')
kwargs = dict()
def needs_target(algo):
    return algo in ['deeplearning', 'gbm', 'glm', 'naive_bayes', 'random_forest']

if needs_target(algorithm):
    target = get_recipe_config().get('target')
    if not target or target == '':
        raise Exception('algorithm ' + algorithm + ' needs a target, please review the recipe\'s settings.')
    kwargs['y'] = train_frame[target]
    kwargs['x'] = train_frame.drop(target)
else:
    kwargs['x'] = train_frame

if valid_frame is not None:
    if needs_target(algorithm):
        kwargs['validation_y'] = valid_frame[target]
        kwargs['validation_x'] = valid_frame.drop(target)
    else:
        kwargs['validation_x'] = valid_frame

## create output_folder and dump model config
output_folder = dataiku.Folder(get_output_names_for_role('output_folder')[0])
output_folder_path = output_folder.get_path()
# clean it: works only on local FS
# for file in os.listdir(output_folder_path):
    # path = os.path.join(output_folder_path, file)
    # if   os.path.isfile(path): os.unlink(path)
    # elif os.path.isdir (path): shutil.rmtree(path)

model_config = {
    'factor_columns':factor_columns,
    'input_type': dataiku.Dataset(train_dataset).get_config()['type'] }
with open(os.path.join(output_folder_path, 'model_config.json'),'w') as file:
    file.write(json.dumps(model_config,indent=4))

## set final parameters, train model
params = get_recipe_config().get('algorithm_parameters','{}')
if params == '': params = '{}'
kwargs.update(json.loads(params))
kwargs['model_id'] = 'DSS.H2O_connector.model.' + output_folder.full_name + '.' + algorithm


algorithms = {
    'autoencoder': h2o.h2o.autoencoder,
    'deeplearning': h2o.h2o.deeplearning,
    'gbm': h2o.h2o.gbm,
    'glm': h2o.h2o.glm,
    'glrm': h2o.h2o.glrm,
    'kmeans': h2o.h2o.kmeans,
    'naive_bayes': h2o.h2o.naive_bayes,
    'prcomp': h2o.h2o.prcomp,
    'random_forest': h2o.h2o.random_forest,
    'svd': h2o.h2o.svd,
}

# print 'Arguments passed to H2O: ', kwargs # This makes the job fail with exception None ??
model = algorithms[algorithm](**kwargs)

## save model summary in output_folder and model to disk
with open(os.path.join(output_folder_path, 'model_summary.txt'),'w') as file:
    orig_stdout = sys.stdout
    sys.stdout = file
    model.show() # this method uses print to write to stdout
    sys.stdout = orig_stdout

h2o.h2o.save_model(
    model,
    saved_model_folder(model_config, output_folder),
    force=True) # "force" means overwrite
