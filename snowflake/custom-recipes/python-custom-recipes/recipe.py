import os
import sys
import json
import dataiku
import urllib.parse as urlparse
import snowflake.connector as sf
from dataiku.customrecipe import *

# Settings
DATASET_IN     = get_input_names_for_role("input_dataset")[0]
DATASET_OUT    = get_output_names_for_role("output_dataset")[0]

AWS_ACCESS_KEY = get_recipe_config().get("aws_access_key")
AWS_SECRET_KEY = get_recipe_config().get("aws_secret_key")

if not AWS_ACCESS_KEY or not AWS_SECRET_KEY:
    # Looking up in Project Variables
    print("[+] AWS Access Key or Secret Key not entered in the Plugin interface. Looking into Project Variables...")    
    dss = dataiku.api_client()
    project = dss.get_project(dataiku.default_project_key())
    variables = project.get_variables()["standard"]
    if "snowflake" in variables:
        if "aws_access_key" in variables["snowflake"] and "aws_secret_key" in variables["snowflake"]:
            print("[+] Found AWS credentials in Project Variables")
            AWS_ACCESS_KEY = variables["snowflake"]["aws_access_key"]
            AWS_SECRET_KEY = variables["snowflake"]["aws_secret_key"]
        else:
            print("[-] Snowflake key found in Project Variables but can not retrieve aws_access_key and/or aws_secret_key.")
            print("[-] Please check and correct your Project Variables.")
            sys.exit("Project Variables error")
    else:
        # Looking into Global Variables
        variables = dss.get_variables()
        if "snowflake" in variables:
            if "aws_access_key" in variables["snowflake"] and "aws_secret_key" in variables["snowflake"]:
                print("[+] Found AWS credentials in Global Variables")
                AWS_ACCESS_KEY = variables["snowflake"]["aws_access_key"]
                AWS_SECRET_KEY = variables["snowflake"]["aws_secret_key"]
        else:
            print("[-] Snowflake key found in Global Variables but can not retrieve aws_access_key and/or aws_secret_key.")
            print("[-] Please check and correct your Global Variables.")
            sys.exit("Global Variables error")
    

# Dataiku Datasets
ds = dataiku.Dataset(DATASET_IN)
out = dataiku.Dataset(DATASET_OUT)


#------------------------------------------------------------------------------
# INPUT DATASET SETTINGS
#------------------------------------------------------------------------------

# Input dataset settings
config = ds.get_config()

if config["formatType"] != 'csv':
    print("[-] Only a CSV format for the input DSS Dataset is supported (you used {}).".format(config["formatType"]))
    print("[-] Please adjust the format. Aborting")
    sys.exit("Format error (CSV needed)")

project_key = config["projectKey"]

# Actual path of the input file on S3
bucket = config["params"]["bucket"]
path = config["params"]["path"].replace("${projectKey}",config["projectKey"])
full_path = "s3://{}{}".format(bucket, path)

# Input file definition
separator = config["formatParams"]["separator"]
skip_rows = config["formatParams"]["skipRowsBeforeHeader"]


#------------------------------------------------------------------------------
# OUTPUT DATASET SETTINGS
#------------------------------------------------------------------------------

# Output configuration
config = out.get_location_info(sensitive_info=True)

# Snowflake credentials & output table
jdbc_url = config["info"]["connectionParams"]["jdbcurl"]
components = urlparse.parse_qs(urlparse.urlparse(jdbc_url).query)

sf_user      = components["user"][0]
sf_password  = components["password"][0]
sf_database  = components["db"][0]
sf_schema    = components["schema"][0]
sf_warehouse = components["warehouse"][0]
sf_account   = urlparse.urlparse(jdbc_url).path.replace("snowflake://", "").replace(".snowflakecomputing.com", "")

output_table = config["info"]["table"].replace("${projectKey}", project_key)


#------------------------------------------------------------------------------
# BULK LOADING TO SNOWFLAKE
#------------------------------------------------------------------------------

cnx = sf.connect(
    user=sf_user,
    password=sf_password,
    account=sf_account,
    warehouse=sf_warehouse,
    database=sf_database,
    schema=sf_schema
)

cur = cnx.cursor()

# Building schema
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
    
schema_out = ", ".join(schema) 

# Actual Snowflake bulkload
print("[+] Create target table ...")
q = """ CREATE OR REPLACE TABLE \"{}\" ({})""".format(output_table, schema_out)
cur.execute(q)

print("[+] Create a file format ...")
q = """CREATE OR REPLACE FILE FORMAT dss_ff
              TYPE = 'CSV'
              FIELD_DELIMITER = '{}'
              SKIP_HEADER = {} ;""".format(separator, skip_rows)
cur.execute(q)

print("[+] Create stage file ...")
q = """CREATE OR REPLACE STAGE dss_stage
       FILE_FORMAT = dss_ff
       URL = '{}'
       CREDENTIALS = (AWS_KEY_ID = '{}' AWS_SECRET_KEY = '{}')""".format(full_path, AWS_ACCESS_KEY, AWS_SECRET_KEY)
cur.execute(q)


print("[+] Loading data...")
q = """COPY INTO \"{}\" FROM @dss_stage 
        ON_ERROR = 'ABORT_STATEMENT' """.format(output_table)
print(q)
cur.execute(q)

# Write recipe outputs
out.write_schema( ds.read_schema() )
print("[+] Loading Done")