from dataiku.runnables import Runnable
import dataiku
import urllib2, sys
import requests
import json
import os
import dl_image_toolbox_utils as utils
import pandas as pd
import constants
import time

# We deactivate GPU for this script, because all the methods only need to 
# fetch information about model and do not make computation
utils.deactivate_gpu()

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()

        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return (100, 'NONE')


    def run(self, progress_callback):

        # Retrieving parameters
        output_folder_name = self.config.get('outputName', '')
        model = self.config.get('model', '')
        architecture, trained_on = model.split('_')
        
        # Creating new Managed Folder if needed
        project = self.client.get_project(self.project_key)
        output_folder_found = False
        
        for folder in project.list_managed_folders():
            if output_folder_name == folder['name']:
                output_folder = project.get_managed_folder(folder['id'])
                output_folder_found = True
                break
        
        if not output_folder_found:
            output_folder = project.create_managed_folder(output_folder_name)

        output_folder_path = dataiku.Folder(output_folder.get_definition()["id"], project_key=self.project_key).get_path()
        
        # Building config file
        config = {
            "architecture": architecture,
            "trained_on": trained_on,
            "extract_layer_default_index": utils.get_extract_layer_index(architecture, trained_on)
        }

        # Downloading weights
        url_to_weights = utils.get_weights_urls(architecture, trained_on)

        def update_percent(percent, last_update_time):
            new_time = time.time()
            if (new_time - last_update_time) > 3:
                progress_callback(percent)
                return new_time
            else:
                return last_update_time

        def download_files_to_managed_folder(folder_path, files_info, chunk_size=8192):
            total_size = 0
            bytes_so_far = 0
            for file_info in files_info:
                response = requests.get(file_info["url"], stream=True)
                total_size += int(response.headers.get('content-length'))
                file_info["response"] = response
            update_time = time.time()
            for file_info in files_info:
                with open(utils.get_file_path(folder_path, file_info["filename"]), "wb") as f:
                    for content in file_info["response"].iter_content(chunk_size=chunk_size):
                        bytes_so_far += len(content)
                        # Only scale to 80% because needs to compute model summary after download
                        percent = int(float(bytes_so_far) / total_size * 80)
                        update_time = update_percent(percent, update_time)
                        f.write(content)

        files_to_dl = [
            {"url": url_to_weights["top"], "filename": utils.get_weights_filename(output_folder_path, config)},
            {"url": url_to_weights["no_top"], "filename": utils.get_weights_filename(output_folder_path, config, "_notop")}
        ]

        if trained_on == constants.IMAGENET:
            # Downloading mapping id <-> name for imagenet classes
            # File used by Keras in all its 'decode_predictions' methods
            # Found here : https://github.com/keras-team/keras/blob/2.1.1/keras/applications/imagenet_utils.py
            imagenet_id_class_mapping_url = "https://s3.amazonaws.com/deep-learning-models/image-models/imagenet_class_index.json"
            imagenet_class_mapping_temp_file = "imagenet_classes_mapping.json"
            files_to_dl.append({"url": imagenet_id_class_mapping_url, "filename": imagenet_class_mapping_temp_file})

        output_folder.put_file(constants.CONFIG_FILE, json.dumps(config))
        download_files_to_managed_folder(output_folder_path, files_to_dl)

        if trained_on == constants.IMAGENET:
            # Convert class mapping from json to csv
            mapping_df = pd.read_json(utils.get_file_path(output_folder_path, imagenet_class_mapping_temp_file), orient="index")
            mapping_df = mapping_df.reset_index()
            mapping_df = mapping_df.rename(columns={"index": "id", 1: "className"})[["id", "className"]]
            mapping_df.to_csv(utils.get_file_path(output_folder_path, constants.MODEL_LABELS_FILE), index=False, sep=",")
            os.remove(utils.get_file_path(output_folder_path, imagenet_class_mapping_temp_file))

        # Computing model info
        utils.save_model_info(output_folder_path)
        
        return "<span>DONE</span>"

