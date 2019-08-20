# This file is the actual code for the Python runnable macro-excel-importer
import dataiku
import pandas as pd
import os
import openpyxl
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

        # Get project and folder containing the Excel files
        client = dataiku.api_client()
        project = client.get_project(self.project_key)

        folder_id = self.config.get("model_folder_id")
        folder = dataiku.Folder(folder_id, project_key=self.project_key)
        folder_path = folder.get_path()

        # List files in folder and get path
        files_list = os.listdir(folder_path)

        # List the datasets in the project
        datasets_in_project = []
        for i in range(len(project.list_datasets())):
            datasets_in_project.append(project.list_datasets()[i]['name'])
        
        # Actions performed
        actions_performed = dict()
        
        for my_file in files_list:
            ## Get file path
            file_path = os.path.join(folder_path, my_file)
            
            ## Get Excel file and load in a pandas dataframe
            sheets_names = pd.ExcelFile(file_path).sheet_names
            for sheet in sheets_names:
                
                ### Rename sheets by "file_sheet"
                ss=openpyxl.load_workbook(file_path)
                ss_sheet = ss.get_sheet_by_name(sheet)
                if not my_file.split(".")[0] in ss_sheet.title:
                    ss_sheet.title = my_file.split(".")[0] + "_" + sheet
                    ss.save(file_path)

             ## If the dataset already exists, delete and replace it
                actions_performed[ss_sheet.title] = "created"
                if ss_sheet.title in datasets_in_project:
                    project.get_dataset(ss_sheet.title).delete()
                    actions_performed[ss_sheet.title] = "replaced"

                ### Create dataset from Excel sheet
                project.create_dataset(ss_sheet.title
                        ,'FilesInFolder'
                        , params={'folderSmartId': folder_id,'path': my_file}
                        , formatType='excel'
                        , formatParams={"xlsx":True, "sheets":"*"+ss_sheet.title,'parseHeaderRow': True})

        
        
        # Output table
        from dataiku.runnables import Runnable, ResultTable
        rt = ResultTable()
        rt.add_column("actions", "Actions", "STRING")

        # Actions : "dataset" has been created or replaced
        for i in range(len(actions_performed)):
            record = []
            record.append(actions_performed.keys()[i] + " has been " + actions_performed.values()[i])
            rt.add_record(record)

        return rt