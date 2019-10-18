# This file is the actual code for the Python runnable macro-excel-importer
import dataiku
import pandas as pd
import os
import openpyxl
import time
from dataiku.runnables import Runnable, ResultTable

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
        return (100, 'FILES')

    def run(self, progress_callback):

        def update_percent(percent, last_update_time):
            new_time = time.time()
            if (new_time - last_update_time) > 3:
                progress_callback(percent)
                return new_time
            else:
                return last_update_time

        # Get project and folder containing the Excel files
        client = dataiku.api_client()
        project = client.get_project(self.project_key)

        folder_id = self.config.get("model_folder_id")
        overwrite = self.config.get("overwrite", False)

        folder = dataiku.Folder(folder_id, project_key=self.project_key)
        folder_path = folder.get_path()

        macro_creates_dataset = False # A boolean used to provide an informative message to the user when the macro creates a dataset
        # List files in folder and get path
        files_list = os.listdir(folder_path)

        # List the datasets in the project
        datasets_in_project = []
        for i in range(len(project.list_datasets())):
            datasets_in_project.append(project.list_datasets()[i]['name'])
        
        # Actions performed
        actions_performed = dict()
        num_files = len(files_list)

        update_time = time.time()
        for file_index, my_file in enumerate(files_list):
            
            ## Get file path
            file_path = os.path.join(folder_path, my_file)
            
            ## Get Excel file and load in a pandas dataframe
            sheets_names = pd.ExcelFile(file_path).sheet_names
            for sheet in sheets_names:
                ### Rename sheets by "file_sheet"
                ss=openpyxl.load_workbook(file_path)
                ss_sheet = ss.get_sheet_by_name(sheet)
                title = ss_sheet.title
                
                if not my_file.split(".")[0] in title:
                    title = '_'.join((my_file.split(".")[0] + "_" + sheet).split())
                
                title = '_'.join(title.split())
                title = title.replace(')','')
                title = title.replace('(','')
                
                create_dataset = True
                if title in datasets_in_project:
                    if overwrite:
                        project.get_dataset(title).delete()
                        actions_performed[title] = "replaced"
                    else:
                        create_dataset = False
                        actions_performed[title] = "skipped (already exists)"
                else:
                    actions_performed[title] = "created"
                    macro_creates_dataset = True
                if create_dataset:
                    dataset = project.create_dataset(title
                                    ,'FilesInFolder'
                                    , params={'folderSmartId': folder_id,
                                              'filesSelectionRules': {'mode': 'EXPLICIT_SELECT_FILES', 
                                                                     'explicitFiles': [my_file]}}
                                    , formatType='excel'
                                    , formatParams={"xlsx":True, "sheets":"*"+ss_sheet.title,'parseHeaderRow': True})
                    
                    df = pd.read_excel(file_path, sheet_name=ss_sheet.title, nrows=1000)
                    dataset.set_schema({'columns': [{'name': column, 'type': 'string'} for column, column_type in df.dtypes.items()]})

                percent = 100*float(file_index+1)/num_files
                update_time = update_percent(percent, update_time)

        # Output table
        rt = ResultTable()
        rt.add_column("actions", "Actions", "STRING")

        # Actions : "dataset" has been created or replaced
        for i in range(len(actions_performed)):
            record = []
            record.append(list(actions_performed.keys())[i] + " has been " + list(actions_performed.values())[i])
            rt.add_record(record)
        
        if macro_creates_dataset:
            rt.add_record(["Please refresh this page to see new datasets."])

        return rt