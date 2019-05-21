# This file is the actual code for the Python runnable macro-excel-importer
import dataiku
import pandas as pd
import os
from dataiku.runnables import Runnable



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
        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None

    def run(self, progress_callback):


        # Create dataset using the API
        client = dataiku.api_client()
        model_folder_id = self.config.get("model_folder_id")
        print(model_folder_id)

        # Get folder and path
        folder_id = model_folder_id
        folder = dataiku.Folder(folder_id, project_key=self.project_key)
        folder_path = folder.get_path()

        # List files in folder and get path
        files_list = os.listdir(folder_path)

        # Create dataset using the API
        client = dataiku.api_client()
        project = client.get_project(self.project_key)

        for my_file in files_list:
            file_path = os.path.join(folder_path, my_file)
            # Get Excel file and load in a pandas dataframe
            sheets_names = pd.ExcelFile(file_path).sheet_names
            for sheet in sheets_names:
                #sheet = str(sheet.replace(" ", "_"))
                dataset_name = str(my_file).replace(" ", "_").split(".")[0] + "_" + sheet
                project.create_dataset(dataset_name
                        ,'FilesInFolder'
                        , params={'folderSmartId': folder_id,'path': my_file}
                        , formatType='excel'
                        #, readWriteOptions={'forceSingleOutputFile': False}
                        , formatParams={"xlsx":True, "sheets":"*"+sheet,'parseHeaderRow': True})

