# -*- coding: utf-8 -*-

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

import dataiku
from dataiku.customrecipe import *
from dataiku.core import sql

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
input_dataset = dataiku.Dataset(get_input_names_for_role("main")[0])
output_dataset = dataiku.Dataset(get_output_names_for_role("main")[0])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
input_dataset_loc = input_dataset.get_location_info()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
output_dataset_loc = output_dataset.get_location_info()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
output_dataset.write_schema(input_dataset.read_schema())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def hive_type_to_dss_type(dss_type):
    if dss_type == "date":
        return "timestamp"
    else:
        return dss_type

hive_schema = ["`%s` %s" % (x["name"], hive_type_to_dss_type(x["type"])) for x in input_dataset.read_schema()]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
executor = sql.SQLExecutor2(dataset=output_dataset)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
try:
    executor.query_to_df("DROP TABLE %s" % output_dataset_loc["info"]["table"])
except Exception, e:
    print ("Drop failed, table probably didn't exist: %s" % e)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
executor.query_to_df("""CREATE EXTERNAL TABLE %s (%s) STORED AS TEXTFILE
                     ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
                     LOCATION '%s'""" % (
                                  output_dataset_loc["info"]["table"],
                                  ",".join(hive_schema),
                                  input_dataset_loc["info"]["path"]))