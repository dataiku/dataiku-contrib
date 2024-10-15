import logging
import json, os

import dataiku
from dataiku.core import intercom
from dataiku.base.spark_like import SparkLike
from dataiku.base.sql_dialect import SparkLikeDialect

import base64
from urllib.parse import urlparse, parse_qsl

try:
    from google.oauth2 import service_account, credentials
except ImportError as e:
    raise Exception("Unable to import google libraries. Make sure you are using a code-env where google is installed. Cause: " + str(e))

try:
    from bigframes import Session
    from bigframes.session.clients import ClientsProvider
except ImportError as e:
    raise Exception("Unable to import bigframes libraries. Make sure you are using a code-env where bigframes is installed. Cause: " + str(e))

class DkuBigframesDialect(SparkLikeDialect):

    def __init__(self):
        SparkLikeDialect.__init__(self)

    def _get_to_dss_types_map(self):
        if self._to_dss_types_map is None:
            self._to_dss_types_map = {
                            'Int64': 'bigint',
                            'Numeric': 'double',
                            'Bignumeric': 'double',
                            'Float64': 'float',
                            'Array': 'array',
                            'Bool': 'boolean',
                            'Bytes': 'string',
                            'Date': 'date',
                            'Datetime': 'date',
                            'Geography': 'geometry',
                            'Interval': 'string',
                            'Json': 'string',
                            'Range': 'range',
                            'Struct': 'string',
                            'Timestamp': 'date',  # By default we consider it's a TIMESTAMP_TZ
                            'Time': 'string'
                        }
        return self._to_dss_types_map
        
    def allow_empty_schema_after_catalog(self):
        """Whether specifying a table as (catalog, table) is possible"""
        return True
        
    def identifier_quote_char(self):
        """Get the character used to quote identifiers"""
        return '"'
    
    def _column_name_to_sql_column(self, identifier):
        return col(identifier)
    
    def _python_literal_to_sql_literal(self, value, column_type, original_type=None):
        if column_type == 'date':
            return call_builtin("TO_TIMESTAMP_TZ", str(value))
        else:
            return lit(value)        
    
    def _get_components_from_df_schema(self, df):
        fields = {}
        names = []
        for name in df.dtypes.index:
            col_name = self.unquote_identifier(name)
            names.append(col_name)
            fields[col_name] = {"name":col_name, "datatype":df.dtypes[col_name]}
        return (names, fields)
        
        
# noinspection PyPep8Naming
class DkuBigframes(SparkLike):

    def __init__(self):
        SparkLike.__init__(self)
        self._dialect = DkuBigframesDialect()
        self._connection_type = "BigQuery"
        
    def get_dataframe(self, dataset, session=None):
        """Return a DataFrame configured to read the table that is underlying the specified dataset."""
        dataset_config = dataset.get_config()
        dataset_params = dataset_config["params"]
        if session is None:
            session = self.get_session(dataset_params["connection"], dataset.project_key)

        if dataset_config["type"] != self._connection_type:
            raise ValueError("Dataset is not of type %s" % self._connection_type)

        dataset_info = dataset.get_location_info()["info"]
        logging.debug("Retrieved dataset info: %s " % dataset_info)
        dss_schema = dataset.read_schema(raise_if_empty=False)
        if dataset_params["mode"] == "table":
            table_name = self._get_table_full_name(dataset_info, dataset_config["projectKey"]).replace('"','')
            logging.info("BQ Table name: %s" %table_name)
            result = session.read_gbq(table_name)
            if hasattr(dataset, "read_partitions") and dataset.read_partitions:
                # Build the filter that correspond to the requested partitions (if any)
                partitions_filters = []
                dimensions = dataset_config["partitioning"]["dimensions"]

                for partition in dataset.read_partitions:
                    dimensions_filters = []
                    for index, partition_id in enumerate(partition.split('|')):
                        dimensions_filters.append(self._build_partition_filter_item(dataset, dss_schema, dimensions[index], partition_id))

                    partitions_filters.append(functools.reduce(lambda f1, f2: f1 & f2, dimensions_filters))

                result = result.filter(functools.reduce(lambda f1, f2: f1 | f2, partitions_filters))
        else:
            if hasattr(dataset, "read_partitions") and dataset.read_partitions:
                # Build the filter that correspond to the requested partitions (if any)
                expanded_queries_df = []
                for partition in dataset.read_partitions:
                    expand_sql_request = {
                        "projectKey": dataset.project_key,
                        "datasetName": dataset.short_name,
                        "partition": partition
                    }
                    expanded_query = intercom.jek_or_backend_json_call("datasets/expand-sql-query-for-partition/", data=expand_sql_request)
                    expanded_queries_df.append(session.read_gbq_query(expanded_query))

                result = functools.reduce(lambda df1, df2: df1.unionAll(df2), expanded_queries_df)
            else:
                result = session.read_gbq_query(dataset_info["query"])

        return result
    
    def write_dataframe(self, dataset, df, infer_schema=False, force_direct_write=False, dropAndCreate=False):
        """Writes this dataset (or its target partition, if applicable) from
        a single dataframe.

        This variant only edit the schema if infer_schema is True, otherwise you must
        take care to only write dataframes that have a compatible schema.
        Also see "write_with_schema".

        :param df: input dataframe.
        :param dataset: Output dataset to write.
        :param infer_schema: infer the schema from the dataframe.
        :param force_direct_write: Force writing the dataframe using the direct API into the dataset even if they don't come from the same DSS connection.
        :param dropAndCreate:  if infer_schema and this parameter are both set to True, clear and recreate the dataset structure.
        """
        
        self._check_dataframe_type(df)

        df_connection_name = df._session.dss_connection_name if hasattr(df._session, "dss_connection_name") else None
        dataset_config = dataset.get_config()
        dataset_info = dataset.get_location_info()["info"]
        logging.debug("Retrieved dataset info: %s " % dataset_info)

        # If the destination dataset is of the right type
        # And it uses the same DSS connection as the one used to create the DataFrame
        # Then we directly write into the destination table using direct API, else we raise errors.
        if dataset_config["type"] != self._connection_type:
            raise ValueError("Cannot use direct API to direct write the dataframe into a dataset that is not stored on %s. Use dataset.write_dataframe(df.toPandas()) to write using a degraded mode if the dataframe is not too large." % self._connection_type)
        if dataset_info["connectionName"] != df_connection_name and not force_direct_write:
            raise ValueError("Cannot use direct API to direct write the dataframe into the dataset as they are on different DSS connections. Use force_direct_write=True to ignore this check or call dataset.write_dataframe(df.toPandas()) to write using a degraded mode if the dataframe is not too large.")

        logging.info("Writing dataframe directly with direct API as the destination dataset is based on the same connection as the dataframe.")
        write_mode = dataset.spec_item["appendMode"] and "APPEND" or "OVERWRITE"
        qualified_table_id = self._get_table_full_name(dataset_info, dataset_config["projectKey"]).replace('"','')
        partitioning = dataset_config["partitioning"] if "partitioning" in dataset_config else {}
        dimensions = partitioning["dimensions"] if "dimensions" in partitioning else []

        dss_schema = dataset.read_schema(raise_if_empty=False)
        # For partitioned datasets, add columns for missing partitions
        if len(dimensions) > 0:
            logging.info("Checking for missing partitioning columns")
            column_names, column_fields = self._dialect._get_components_from_df_schema(df)
            for index, dimension in enumerate(dimensions):
                partition_name = dimension["name"]
                partition_col = self._get_schema_column(dss_schema, partition_name)
                if partition_col is None:
                    logging.warn("Dataset '%s' does not contain the partitioning column %s. Schema retrieved: %s "
                                 % (dataset.name, partition_name, dss_schema))
                    raise Exception("Dataset '%s' does not contain the partitioning column %s " % (dataset.name, partition_name))
                if not (partition_name in column_names):
                    logging.debug("Adding missing partitioning column %s" % partition_name)
                    # add column with a "fallback" type (ie: string)
                    df = self._do_with_column(df, partition_name, self._dialect.python_string_to_sql_literal(''))

        # Once new partition columns have potentially been added:
        # Infer & write schema
        if infer_schema:
            dataset.write_schema(self._dialect.get_dss_schema_from_df_schema(df), dropAndCreate)
            dss_schema = dataset.read_schema(raise_if_empty=False)

        if len(dimensions) > 0:
            # at this stage, there won't be missing columns in the schemas (neither dataset nor dataframe)
            column_names, column_fields = self._dialect._get_components_from_df_schema(df)
            partition_ids = dataset.writePartition.split('|')
            for index, dimension in enumerate(dimensions):
                partition_name = dimension["name"]
                partition_col = self._get_schema_column(dss_schema, partition_name)
                partition_id = partition_ids[index]
                if dimension["type"] == "time":
                    partition_id = self._compute_time_partition_id(dimension["params"]["period"], partition_id, partition_col["type"])
                #print("partition written %s -> %s" % (json.dumps(partition_col), partition_id))
                logging.debug("Setting value of partitioning column %s with value %s" % (partition_name, partition_id))
                partition_value = self._dialect.python_string_to_sql_literal(partition_id, partition_col["type"], partition_col.get("originalType"))
                # beware: the partition_value may not be of the right type
                old_datatype = column_fields[partition_name]["datatype"]
                #print("Keep %s" % old_datatype)
                df = self._do_with_column(df, partition_name, partition_value)

        prepare_sql_request = {
            "projectKey": dataset.project_key,
            "datasetName": dataset.short_name,
            "partition": dataset.writePartition,
            "writeMode": write_mode
        }
        logging.info("Preparing SQL table for write with %s" % prepare_sql_request)
        intercom.jek_or_backend_void_call("datasets/prepare-sql-table-for-write/", data=prepare_sql_request)

        # cast columns to their final types
        # important: AFTER prepare-sql-table-for-write so that the table exists
        df = self._cast_to_target_types(df, dss_schema, qualified_table_id)

        logging.info("Storing dataframe in table with INSERT INTO")
        df.to_gbq(destination_table = qualified_table_id, if_exists="replace")


    def _create_session(self, connection_name, connection_info, project_key=None):

        credentials = self._get_credentials(connection_name, connection_info)
        connection_params = connection_info.get_resolved_params()
        
        bq_client_provider = ClientsProvider(project=connection_info.get_params()["projectId"],credentials=credentials)
        
        logging.info("Establishing BigQuery session")
        session = Session(clients_provider=bq_client_provider)
        logging.info("BigQuery session established")

        # Execute post connect statements if any
        if "postConnectStatementsExpandedAndSplit" in connection_params and len(connection_params["postConnectStatementsExpandedAndSplit"]) > 0:
            for statement in connection_params["postConnectStatementsExpandedAndSplit"]:
                logging.info("Executing statement: %s" % statement)
                session.bqclient.query(statement).collect()
                logging.info("Statement done")

        session.dss_connection_name = connection_name  # Add a dynamic attribute to the session to recognize its DSS connection later on
        return session
        
    def _get_credentials(self, connection_name, connection_info):
        """Check if the dataframe is of the correct type"""
        
        connection_params = connection_info.get_resolved_params()
        
        if connection_params['authType'] == "KEYPAIR":
            if 'appSecretContent' in connection_params:
                keyRaw = connection_params['appSecretContent']
            elif 'keyPath' in connection_params:
                keyRaw = connection_params['keyPath']
            else:
                raise ValueError("No keypair found in %s connection. Please refer to DSS Service Account Auth documentation.".format(connection_name))
            key = json.loads(keyRaw)
            bq_credentials = service_account.Credentials.from_service_account_info(key)
            
        elif connection_params['authType'] == "OAUTH":
            if 'accessToken' not in connection_info['resolvedOAuth2Credential']:
                raise ValueError("No accessToken found in %s connection. Please refer to DSS OAuth2 credentials documentation.".format(connection_name))
            accessToken = connection_info['resolvedOAuth2Credential']['accessToken']
            bq_credentials = credentials.Credentials(accessToken)
    
        else:
            raise ValueError("Unsupported authentication type '%s'.".format(connection_params['authType']))
        
        return bq_credentials

    def _check_private_key_file_ext(self, file_path):
        """Check if the file is a JSON file"""
        if not os.path.splitext(file_path).endswith("json"):
            raise ValueError("File is not a valid JSON File. Note p12 format is not supported for bigframes")

    def _check_dataframe_type(self, df):
        """Check if the dataframe is of the correct type"""
        if not df.__class__.__module__.startswith("bigframes"):
            raise ValueError("Dataframe is not a bigframes dataframe. Use dataset.write_dataframe() instead.")

    def _do_with_column(self, df, column_name, column_value):
        """Add or set a column in the dataframe"""
        return df.withColumn(self._dialect.quote_identifier(column_name), column_value)

    def _get_table_schema(self, schema, connection_params):
        if schema and schema.strip():
            return schema
        return self._get_connection_param(connection_params, "defaultSchema", "schema")

    def _get_table_catalog(self, catalog, connection_params):
        if catalog and catalog.strip():
            return catalog
        return self._get_connection_param(connection_params, "db", "db")