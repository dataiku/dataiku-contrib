# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.core.sql import SQLExecutor2, HiveExecutor, ImpalaExecutor

# Configure all
input_name = get_input_names_for_role('main')[0]
output_name = get_output_names_for_role('main')[0]

dataset = dataiku.Dataset(input_name)
output = dataiku.Dataset(output_name)
dataset_config = dataset.read_config()

compute_most_frequent = bool(get_recipe_config()['compute_most_frequent'])

# Dispatch the columns by type
num_columns = []
date_columns = []
str_columns = []
dispatch= {
    "int" : num_columns,
    "bigint" : num_columns,
    "smallint" : num_columns,
    "tinyint" : num_columns,
    "double" : num_columns,
    "float" : num_columns,
    "date" : date_columns
}
for col in dataset.read_schema():
    print col["type"]
    dispatch.get(col["type"], str_columns).append(col["name"])

# Prepare the executors
is_hivelike = False
if dataset_config["type"] in ["MySQL", "PostgreSQL"]:
    q = '"'
    sqlexec = SQLExecutor2(dataset=dataset)
    # Fixme: this is actually wrong ...
    table = dataset.short_name
elif dataset_config["type"] == "HDFS":
    q = '`'
    sqlexec = HiveExecutor(dataset=dataset)
    is_hivelike = True
    table = dataset.short_name
else:
    raise Exception("Unsupported input dataset type")


# Generate a single query for all numerical columns
# And also in same query: string + num columns: one pass for cardinality and nmissing
chunks = []
for col in num_columns:
    chunks.append("MIN(%s%s%s) as %s%s_min%s" % (q, col, q, q, col, q))
    chunks.append("MAX(%s%s%s) as %s%s_max%s" % (q, col, q, q, col, q))
    chunks.append("AVG(%s%s%s) as %s%s_avg%s" % (q, col, q, q, col, q))

for col in str_columns + num_columns:
    chunks.append("count(distinct %s%s%s) as %s%s_distinct%s" % (q, col, q, q, col, q))
    hive_chunk = ""
    # Also consider empty as null on Hivelike
    if col in str_columns and is_hivelike:
        hive_chunk = "when %s%s%s = '' then 1" % (q, col, q)
    chunks.append("sum (case when %s%s%s is null then 1 %s else 0 end) as %s%s_missing%s" % (q, col, q, hive_chunk, q, col, q))

if len(chunks) > 0:
    query = "SELECT COUNT(*) AS global_count, %s FROM %s%s%s" % (",".join(chunks), q, dataset.short_name, q)
    print "EXECUTING : %s" % query
    df = sqlexec.query_to_df(query)
    master_data = df.iloc[0]
    print "MASTER DATA: %s" % master_data
else:
    master_data = {}


# Most frequent on all columns must be handled one at a time for the moment
if compute_most_frequent:
    most_frequent = {}
    for col in str_columns + num_columns:
        query = "select %s%s%s as val, COUNT(*) as count FROM %s%s%s GROUP BY %s%s%s" % (q,col,q, q, dataset.short_name, q, q,col, q)
        df = sqlexec.query_to_df(query)
        most_frequent[col] = (df.iloc[0]["val"], df.iloc[0]["count"])


# Prepare the output
out = []
for col in dataset.read_schema():
    colout = {}
    cname = col["name"]
    oname = col["name"]
    if is_hivelike:
        cname = cname.lower()
    colout["name"] = cname

    if col["name"] in num_columns:
        colout['num_min'] = master_data["%s_min" % cname]
        colout["num_max"] = master_data["%s_max" % cname]
        colout["num_avg"] = master_data["%s_avg" % cname]

    colout["cardinality"] = master_data["%s_distinct" % cname]
    colout["nb_missing"] = master_data["%s_missing" % cname]

    if compute_most_frequent:
        colout["most_frequent_value"] = most_frequent[oname][0]
        colout["most_frequent_count"] = most_frequent[oname][1]
        colout["most_frequent_value_ratio"] = most_frequent[oname][1] / master_data["global_count"]

    out.append(colout)

# Write
columns = ["name", "cardinality", "nb_missing"]
if compute_most_frequent:
    columns.extend(["most_frequent_value", "most_frequent_count", "most_frequent_value_ratio"])
columns.extend(["num_min", "num_max", "num_avg"])

output.write_with_schema(pd.DataFrame(out, columns = columns))