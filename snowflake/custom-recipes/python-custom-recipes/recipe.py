import os
import sys
import json
import re
import dataiku
import urllib.parse as urlparse
import snowflake.connector as sf
from dataiku.customrecipe import *
from dataiku.core.intercom import backend_json_call

# Settings
DATASET_IN     = get_input_names_for_role("input_dataset")[0]
DATASET_OUT    = get_output_names_for_role("output_dataset")[0]
APPEND_NOT_OVERWRITE = get_recipe_config().get("append_instead_of_overwrite", False)
USE_INPUT_CONNECTION = get_recipe_config().get("use_input_connection", False)
USE_PROJECT_VARIABLES = get_recipe_config().get("use_project_variables", False)

AWS_ACCESS_KEY = None
AWS_SECRET_KEY = None

# Dataiku Datasets
ds = dataiku.Dataset(DATASET_IN)
out = dataiku.Dataset(DATASET_OUT)

# Dataset configurations
config_in = ds.get_config()
config_out = out.get_location_info(sensitive_info=True)["info"]


if USE_INPUT_CONNECTION:
    print("[+] Use the S3 credentials in the Input's connection ({})".format(DATASET_IN))
    connection_name = config_in["params"]["connection"]
    input_connection = backend_json_call("connections/get-details", data={
        "connectionName": connection_name
    })["params"]
    if input_connection["useDefaultCredentials"] is True:
        print("[-] Found the connection {} but it is configured to Use Default Credentials.".format(connection_name))
        print("[-] Connections configured that way do not make aws_access_key and aws_secret_key available.")
        print("[-] If the Input can't use a non-Default Credential connection, use the other authentication options in the Plugin interface.")
        sys.exit("AWS S3 Credential error")
    if input_connection["encryptionMode"] != "NONE":
        print("[-] Found the connection {} but it is configured to use encryption which is not currently supported.".format(connection_name))
        sys.exit("AWS S3 Credential error")
    
    AWS_ACCESS_KEY = input_connection["accessKey"]
    AWS_SECRET_KEY = input_connection["secretKey"]
elif USE_PROJECT_VARIABLES:
    print("[+] Use S3 credentials defined as Local, Project, or Global Variables. First, looking in Local Variables...")
    dss = dataiku.api_client()
    project = dss.get_project(dataiku.default_project_key())
    variables = project.get_variables()
    if "snowflake" in variables["local"]:
        if "aws_access_key" in variables["local"]["snowflake"] and "aws_secret_key" in variables["local"]["snowflake"]:
            print("[+] Found AWS credentials in Local Variables")
            AWS_ACCESS_KEY = variables["local"]["snowflake"]["aws_access_key"]
            AWS_SECRET_KEY = variables["local"]["snowflake"]["aws_secret_key"]
        else:
            print("[-] 'snowflake' key found in Local Variables but could not retrieve aws_access_key and/or aws_secret_key.")
            print("[-] Please check and correct your Local Variables.")
            sys.exit("Local Variables error")
    elif "snowflake" in variables["standard"]:
        if "aws_access_key" in variables["standard"]["snowflake"] and "aws_secret_key" in variables["standard"]["snowflake"]:
            print("[+] Found AWS credentials in Project Variables")
            AWS_ACCESS_KEY = variables["standard"]["snowflake"]["aws_access_key"]
            AWS_SECRET_KEY = variables["standard"]["snowflake"]["aws_secret_key"]
        else:
            print("[-] 'snowflake' key found in Project Variables but could not retrieve aws_access_key and/or aws_secret_key.")
            print("[-] Please check and correct your Project Variables.")
            sys.exit("Project Variables error")
    else:
        # Looking into Global Variables
        print("[+] Could not find 'snowflake' in Project Variables so looking in Global Variables...")
        variables = dss.get_variables()
        if "snowflake" in variables:
            if "aws_access_key" in variables["snowflake"] and "aws_secret_key" in variables["snowflake"]:
                print("[+] Found AWS credentials in Global Variables")
                AWS_ACCESS_KEY = variables["snowflake"]["aws_access_key"]
                AWS_SECRET_KEY = variables["snowflake"]["aws_secret_key"]
            else:
                print("[-] 'snowflake' key found in Global Variables but could not retrieve aws_access_key and/or aws_secret_key.")
                print("[-] Please check and correct your Global Variables.")
                sys.exit("Global Variables error")                
        else:
            print("[-] 'snowflake' key was not found in Project or Global Variables.")
            print("[-] Please check and correct your Project or Global Variables.")
            sys.exit("Project or Global Variables error")
else:
    print("[+] Use the S3 credentials specified in the Plugin interface.")
    AWS_ACCESS_KEY = get_recipe_config().get("aws_access_key")
    AWS_SECRET_KEY = get_recipe_config().get("aws_secret_key")
    if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
        print("[-] S3 credentials were not provided in the Plugin interface.")
        sys.exit("Plugin Variables error")




#------------------------------------------------------------------------------
# INPUT DATASET SETTINGS
#------------------------------------------------------------------------------

# Input dataset settings

if config_in["formatType"] != 'csv':
    print("[-] Only a CSV format for the input DSS Dataset is supported (you used {}).".format(config_in["formatType"]))
    print("[-] Please adjust the format. Aborting")
    sys.exit("Format error (CSV needed)")

project_key = config_in["projectKey"]

# Actual path of the input file on S3
bucket = config_in["params"]["bucket"]
path = config_in["params"]["path"].replace("${projectKey}", project_key)
full_path = "s3://{}{}".format(bucket, path)


# Input file definition
separator = config_in["formatParams"]["separator"]
skip_rows = config_in["formatParams"]["skipRowsBeforeHeader"]


#------------------------------------------------------------------------------
# OUTPUT DATASET SETTINGS
#------------------------------------------------------------------------------


# Snowflake credentials & output table
jdbc_url_parsed = urlparse.urlparse(config_out["connectionParams"]["jdbcurl"])
components = urlparse.parse_qs(jdbc_url_parsed.query)       

sf_cnx_args = {
    "user": components["user"][0],
    "password": components["password"][0],
    "warehouse": components["warehouse"][0],
    "database": components["db"][0]
}

m = re.match(r'snowflake://(?P<account>[^.]+)(\.(?P<region>[\w\d\-]+))?\.snowflakecomputing.com', jdbc_url_parsed.path, re.IGNORECASE)

sf_cnx_args["account"] = m.group("account")

if m.group("region"):
    sf_cnx_args["region"] = m.group("region")

if "schema" in components:
    sf_cnx_args["schema"] = components["schema"][0]
    print("[+] Connect using Schema '{}'".format(sf_cnx_args["schema"]))

if "role" in components:
    sf_cnx_args["role"] = components["role"][0]
    print("[+] Connect using Role '{}'".format(sf_cnx_args["role"]))


print("[+] Connecting to SF DB '{database}' using Warehouse '{warehouse}', and User '{user}'.".format(**sf_cnx_args))

output_table = config_out["table"].replace("${projectKey}", project_key)
output_schema = config_out["schema"].replace("${projectKey}", project_key)


#------------------------------------------------------------------------------
# BULK LOADING TO SNOWFLAKE
#------------------------------------------------------------------------------

# Building schema if we need to
table_def = "CREATE "
if APPEND_NOT_OVERWRITE:
    table_def += "TABLE IF NOT EXISTS "
else:
    table_def += "OR REPLACE TABLE "

fieldSetterMap = {
    'boolean':  'BOOLEAN',
    'tinyint':  'SMALLINT',
    'smallint': 'SMALLINT',
    'int':      'INTEGER',
    'bigint':   'BIGINT',
    'float':    'FLOAT',
    'double':   'FLOAT',
    'date':     'VARCHAR',
    'string':   'VARCHAR',
    'array':    'VARCHAR',
    'map':      'VARCHAR',
    'object':   'VARCHAR'
}

schema = []

for column in ds.read_schema():
    _name = column["name"]
    _type = fieldSetterMap.get(column["type"], "VARCHAR")
    s = "\"{}\" {}".format(_name, _type)
    schema.append(s)

table_def += "\"{}\".\"{}\" ( ".format(output_schema, output_table)
table_def += ", ".join(schema) 
table_def += " )"

def try_execute(connection, query, clean_query = None):
    """Execute the given query on the given connection. This will also print the 
    query (or clean_query), provide more detailed error information, and close 
    connection's cursor.
    
    Args:
        connection (:obj:`Connection`): an open Snowflake connection
        query (str): the query to execute
        clean_query (str, optional): a sanitised version of query suitable for output in 
            application logs. Defaults to `query`.
    """
    print("[+] Executing Snowflake Query: {}".format(clean_query if clean_query else query))
    cur = connection.cursor()
    try:
        cur.execute(query)
    except sf.errors.ProgrammingError as e:
        print('[-] Snowflake Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        sys.exit("Snowflake Error")
    finally:
        cur.close()

# Now that we have a table schema, connect to Snowflake
with sf.connect(**sf_cnx_args) as cnx:
    # Actual Snowflake bulkload
    print("[+] Create target table ...")
    try_execute(cnx, table_def)
    
    qry_args = {
        "field_delim": separator,
        "skip_first": skip_rows,
        "schema": output_schema,
        "table": output_table,
        "s3path": full_path,
        "access_key": AWS_ACCESS_KEY,
        "secret_key": AWS_SECRET_KEY,
        "force_reload": APPEND_NOT_OVERWRITE
    }

    print("[+] Loading data...")
    # While it may be more appropriate to create an EXTERNAL STAGE and FILE FORMAT first, in practice, 
    # it can lead to thread safety issues if you try to run this plugin in parallel
    q = """COPY INTO \"{schema}\".\"{table}\" FROM '{s3path}'
            FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_DELIMITER = '{field_delim}'
                SKIP_HEADER = {skip_first}
            )
            CREDENTIALS = (AWS_KEY_ID = '{access_key}' AWS_SECRET_KEY = '{secret_key}')
            ON_ERROR = 'ABORT_STATEMENT' 
            FORCE = {force_reload};""".format(**qry_args)
    try_execute(cnx, q, q.replace(qry_args["access_key"], "******").replace(qry_args["secret_key"], "*******"))

# Write recipe outputs
out.write_schema( ds.read_schema() )
print("[+] Loading Done")
