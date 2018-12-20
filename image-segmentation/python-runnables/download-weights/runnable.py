# This file is the actual code for the Python runnable download-weights
from dataiku.runnables import Runnable
import requests
import dataiku
import os

try :
    import pycocotools
except : 
    print("##### WARNING ##### Couldn't find pycocotools in installed packages : will proceed to installing the package in plugin codenv")
    client = dataiku.api_client()
    codenv = client.get_code_env('python','plugin_image-segmentation_managed')
    codenv_def = codenv.get_definition()
    codenv_def['specPackageList'] = 'git+https://github.com/waleedka/coco.git#subdirectory=PythonAPI'
    codenv.set_definition(codenv_def)
    
    print('#'*50)
    print('Start updating codenv plugin_image-segmentation_managed with git+https://github.com/waleedka/coco.git#subdirectory=PythonAPI')
    codenv.update_packages()
    print ('Codenv updated')
    
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
        return None

    def run(self, progress_callback):
        # Retrieving parameters
        output_folder_name = self.config.get('outputName', '')
        
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
        
        os.chmod(output_folder_path,0o755)
        for root, dirs, files in os.walk(output_folder_path):  
            for momo in dirs:  
                os.chmod(os.path.join(root, momo), 0o644)
            for momo in files:
                os.chmo(os.path.join(root, momo), 0o644)
        
        def download_weights (folder, weights_url, name):
            r = requests.get(weights_url, allow_redirects=True)
            folder.put_file(name,r.content)
                
        model_urls =['https://github.com/matterport/Mask_RCNN/releases/download/v2.0/mask_rcnn_coco.h5',\
                    'https://github.com/fchollet/deep-learning-models/releases/download/v0.2/resnet50_weights_tf_dim_ordering_tf_kernels_notop.h5']
                
        for url in model_urls:
            name = url.split('/')[-1]
            download_weights(output_folder, url, name)            
        
            
        return "<span>DONE</span>"
        