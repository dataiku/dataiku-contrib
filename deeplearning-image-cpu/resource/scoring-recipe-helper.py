import dataiku
import glob
import pandas as pd
import os

def do(payload, config, plugin_config, inputs):
    if "method" not in payload:
        return {}

    client = dataiku.api_client()

    if payload["method"] == "get-valid-csv-filenames":

        required_columns = ["id", "className"]
        sep = ","

        # Retrieving model folder
        model_folder_full_name = [inp for inp in inputs if inp["role"] == "modelFolder"][0]["fullName"]
        model_folder = dataiku.Folder(model_folder_full_name).get_path()

        csv_files_root_mf = glob.glob(model_folder + "/*.csv")

        # Filtering out files without required columns
        csv_valid_filenames = []
        for f in csv_files_root_mf:
            schema = retrieve_schema_from_pandas_compatible_csv_file(f, sep)
            if len([col for col in required_columns if col not in schema]) == 0 :
                valid_file = {
                    "path": f,
                    "name": os.path.basename(f)
                }
                csv_valid_filenames.append(valid_file)

    return {"csv_valid_filenames": csv_valid_filenames}


def retrieve_schema_from_pandas_compatible_csv_file(file_path, sep):
    try :
        df = pd.read_csv(file_path, sep=sep, nrows=0)
        return df.columns
    except Exception as e:
        print "Unexpected exception : {}".format(e.message)
        return []