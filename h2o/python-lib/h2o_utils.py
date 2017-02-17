# coding: utf-8
import dataiku, os, h2o, json, socket
import pandas as pd
from dataiku.customrecipe import *

def has_data(file):
    return file['size'] > (20 if file['path'].endswith('.gz') else 0)
# .gz files containing no data might cause the error
# “(...) by class water.parser.ParseSetup$GuessSetupTsk; class water.exceptions.H2OParseSetupException:
# Problem parsing foo.gz Cannot determine file type.”.
# This can happen when one partition created by DSS is empty, or when a parallel worker has no data.
# So, we don't pass those files to H2O.

def DSS_dataset_to_H2O_frame(dataset_name):   #, partition_id = None
    """This function passes the path of the data files to H2O (it does not stream the data through Python)."""
    dataset = dataiku.Dataset(dataset_name)
    settings = dataset.get_config()
    if settings['type'] not in ['Filesystem', 'UploadedFiles', 'HDFS']:
        print 'Warning: Datasets of type '+settings['type']+' are not supported for now. '
        'Supported types are Filesystem, UploadedFiles and HDFS.'
    separator = settings['formatParams'].get('separator',"").decode('unicode_escape')
    print 'separator: <' + separator.encode('unicode_escape') + '>'
    if separator == '\t':
        print "Warning: H2O does not seems to support empty columns when the separator is tab."
    col_names = [col['name'] for col in settings['schema']['columns']]
    dataset_path = dataset.get_location_info()['info']['path'].encode('utf-8')
    pathsByPartition = dataset.get_files_info()['pathsByPartition']
    partitions = dataset.read_partitions if dataset.read_partitions else ['NP']
    files = [file for partition in partitions for file in pathsByPartition[partition]]
    filepaths = [dataset_path + file['path'] for file in files if has_data(file)]
    print "filepaths:"
    for f in filepaths:
        print f
    return h2o.import_file(
        path = filepaths,
        destination_frame = 'DSS.H2O_connector.dataset.' + dataset.full_name + '.' + '/'.join(partitions),
        header = 0 if 'parseHeaderRow' not in settings['formatParams'] else 1 if settings['formatParams']['parseHeaderRow'] else -1,
        sep = separator,
        col_names = col_names,
        col_types=None,
        na_strings=None
        # ,parse_type= 'CSV' if settings['formatType']=='csv' else None
        )

def saved_model_folder(model_config, output_folder):
    """Return the path of a folder to save/retrieve the H2O model."""
    if model_config['input_type'] == 'HDFS':
        HDFS_models_path = get_plugin_config().get('hdfs_models_path')
        return 'hdfs://' + os.path.join(HDFS_models_path, output_folder.full_name)
    else:
        return os.path.join(output_folder.get_path(), 'saved_model/')
