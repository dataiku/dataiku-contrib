# coding: utf-8
import dataiku
from dataiku.customrecipe import *
import pandas as pd
import os, sys, json, h2o, re
from h2o_utils import DSS_dataset_to_H2O_frame, saved_model_folder

## load model config and init H2O
input_folder = dataiku.Folder(get_input_names_for_role('input_folder_containing_model')[0])
with open(os.path.join(input_folder.get_path(), 'model_config.json')) as file:
    model_config = json.load(file)

h2o.init(
    ip =   get_plugin_config()['h2o_server_ip'],
    port = get_plugin_config()['h2o_server_port'])

## load input frame
output_dataset = dataiku.Dataset(get_output_names_for_role('output_dataset')[0])
input_name = get_input_names_for_role('dataset_to_score')[0]
input_frame = DSS_dataset_to_H2O_frame(input_name)
print "input_frame id:", input_frame.frame_id
print "input_frame types:", input_frame.types
for col in model_config['factor_columns']:
    input_frame[col] = input_frame[col].asfactor()

## load model
def find_model_id(folder):
    with open(os.path.join(folder.get_path(), 'model_summary.txt')) as file:
        for line in file:
            match = re.match('Model Key:  (DSS\.H2O_connector\.model\.'+folder.full_name+'\..*)$', line)
            if match:
                return match.group(1)
    raise Exception('Could not find model id in model_summary.txt')

model_id = find_model_id(input_folder)

try:
    model = h2o.h2o.get_model(model_id)
except EnvironmentError as e:
    print 'Model key unknown to H2O:\n“',e,'”\n','Will thus load from saved model.'
    path = os.path.join(saved_model_folder(model_config, input_folder), model_id)
    model = h2o.h2o.load_model(path=path)

## compute predictions and add input columns to output
predict_frame = model.predict(input_frame)
print "predict_frame id:", predict_frame.frame_id
print "predict_frame types:", predict_frame.types
sys.stdout.flush()

for col in get_recipe_config()["columns_to_copy_to_output"]:
    predict_frame[col] = input_frame[col]

## save output
def DSS_type(col_name):
    return {
        'enum':'string',
        'string':'string',
        'int':'bigint',
        'real':'double',
        'time':'string', # todo
        'uuid':'string'
        }[predict_frame.type(col_name)]

output_dataset.write_schema([{'name':name, 'type':DSS_type(name)} for name in predict_frame.col_names])


input_type = dataiku.Dataset(input_name).get_config()['type']
output_type = output_dataset.get_config()['type']
if input_type != output_type:
    print 'Distinct locations between input ('+input_type+') and output ('+output_type+'), using pandas Dataframes for output.'
    i=0
    chunksize = 1000
    writer = output_dataset.get_writer()
    while i < predict_frame.nrow:
        chunk = predict_frame[ i:min(predict_frame.nrow,i+chunksize), : ]
        writer.write_dataframe(chunk.as_data_frame(use_pandas=True))
        i += chunksize
    writer.close()

else:
    print 'Same location between input ('+input_type+') and output ('+output_type+'), using h2o.h2o.export_file for output.'
    def partitioning_subpath(partition_scheme, partition_id):
        if not partition_scheme['dimensions']: return "/"
        def pattern(dimension):
            if dimension['type'] == 'value': return '%{'+dimension['name']+'}'
            if dimension['type'] != 'time': raise Exception('unsupported dimension type ' + dimension['type'])
            return '%' + dimension['params']['period'][0]
        result = partition_scheme['filePathPattern'].replace(".*","")
        current_dims = iter(partition_id.split('|'))
        for dim in partition_scheme['dimensions']:
            result = result.replace(pattern(dim), next(current_dims))
        print "partitioning_subpath:", result
        return result

    path = output_dataset.get_location_info()['info']['path'] \
        + partitioning_subpath(output_dataset.get_config()['partitioning'], output_dataset.writePartition)
    # H2O creates the directory on hdfs, but not on a local filesystem (?)
    if output_dataset.get_config()['type'] in ['Filesystem', 'UploadedFiles']:
        if not os.path.exists(path):
            os.makedirs(path)
        path = path + "out.csv"
    print "output_path: ", path
    print "predict_frame id: ", predict_frame.frame_id

    h2o.h2o.export_file(predict_frame, path, force=True) # Force means overwrite
