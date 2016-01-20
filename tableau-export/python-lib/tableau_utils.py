# -*- coding: utf-8 -*-
from tableausdk import *
from tableausdk.Extract import *
from tableausdk.Server import *
# Because of a bug in Tableau, we need to load Tableau before pandas.
# See http://community.tableau.com/thread/156790 for more info

import dataiku
import os
from dataiku.customrecipe import *
from datetime import datetime


typeMap = {
    'tinyint': Type.INTEGER,
    'smallint':Type.INTEGER,
    'int':     Type.INTEGER,
    'bigint':  Type.INTEGER,
    'float':   Type.DOUBLE,
    'double':  Type.DOUBLE,
    'boolean': Type.BOOLEAN,
    'string':  Type.UNICODE_STRING,
    'date':    Type.DATETIME,
    'array':   Type.UNICODE_STRING,
    'map':     Type.UNICODE_STRING,
    'object':  Type.UNICODE_STRING
    }

def convert_type(type):
    return typeMap.get(type,Type.UNICODE_STRING)

fieldSetterMap = {
    'boolean':  lambda row, colNo, value: row.setBoolean(col, val),
    'tinyint':  lambda row, colNo, value: row.setInteger(col, int(val)),
    'smallint': lambda row, colNo, value: row.setInteger(col, int(val)),
    'int':      lambda row, colNo, value: row.setInteger(col, int(val)),
    'bigint':   lambda row, colNo, value: row.setInteger(col, int(val)),
    'float':    lambda row, colNo, value: row.setDouble (col, float(val)),
    'double':   lambda row, colNo, value: row.setDouble (col, float(val)),
    'date':     lambda row, colNo, value: row.setDateTime(col, val.year, val.month, val.day, val.hour, val.minute, val.second, val.microsecond),
    'string':   lambda row, colNo, value: row.setString(col, val),
    'array':    lambda row, colNo, value: row.setString(col, val),
    'map':      lambda row, colNo, value: row.setString(col, val),
    'object':   lambda row, colNo, value: row.setString(col, val),
}

def makeTableDefinition(schema):
    # todo partition
    tableDef = TableDefinition()
    tableDef.setDefaultCollation(Collation.EN_GB)
    for col in schema:
        tableDef.addColumn(col['name'], convert_type(col['type']))
    return tableDef

def insertData(dataset_in, table_out):
    tableDef = table_out.getTableDefinition()
    schema = dataset_in.read_schema()
    nbCol = len(schema)

    for input_row in dataset_in.iter_rows(log_every=10000):
        output_row = Row(tableDef)
        for colNo in range(nbCol):
            data = input_row[schema[colNo]['name']]
            try: fieldSetterMap[schema[colNo]['type']](output_row, colNo, data)
            except: pass
        table_out.insert(output_row)

def output_filename():
    result = get_recipe_config().get("tde_file_name", "output.tde")
    if not result.endswith('.tde'):
        result = result + '.tde'
    return result

def tde_export():
    print "Start export to TDE"
    input_name = get_input_names_for_role('input')[0]
    input_dataset =  dataiku.Dataset(input_name)
    input_schema = input_dataset.read_schema()
    partitions = input_dataset.list_partitions(raise_if_empty=False)
    if partitions not in [[], [u'NP']]:
        raise Exception("Due to the current APIs, this plugin cannot support partitioned input "
            "(and it seems the input dataset " +input_name+ " is partitioned). "
            "A workaround is to first run a sync recipe "
            "from " +input_name+ " into a non partitioned dataset, "
            "then take the latter as input for tde export.")
    output_name = get_output_names_for_role('output_folder')[0]
    output_folder = dataiku.Folder(output_name)
    output_path = output_folder.get_path()

    os.chdir(output_path)

    # Clean output dir. We assume there is no subfolder.
    # (because this recipe never creates one. If there is, better fail than remove someone else's data)
    for file in os.listdir(output_path):
        os.remove(file)

    ExtractAPI.initialize()

    with Extract(output_filename()) as extract:
        assert(not extract.hasTable('Extract'))
        tableDef = makeTableDefinition(input_schema)
        table = extract.addTable('Extract', tableDef)
        insertData(input_dataset, table)
        extract.close()
        ExtractAPI.cleanup()
    print "End export to TDE"

def upload():
    print "Start upload to Tableau server"
    ServerAPI.initialize()
    serverConnection = ServerConnection()
    proxyUsername = get_recipe_config().get("proxy_username",'')
    proxyPassword = get_recipe_config().get("proxy_password",'')
    if proxyUsername != '':
        serverConnection.setProxyCredentials(proxyUsername, proxyPassword)
    serverConnection.connect(
        get_recipe_config()['server_url'],
        get_recipe_config()['username'],
        get_recipe_config().get('password',''),
        get_recipe_config().get('site_id','') );

    project = get_recipe_config().get('project','default')
    if project == '':
        project = 'default'
    output_table = get_recipe_config().get('output_table','DSS_extract')
    if output_table == '':
        output_table = 'DSS_extract'
    serverConnection.publishExtract(
        output_filename(),
        project,
        output_table,
        True ); # overwrite
    serverConnection.disconnect();
    serverConnection.close();
    ServerAPI.cleanup();
    print "End upload to Tableau server"
