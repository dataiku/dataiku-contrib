# This file is the actual code for the Python runnable create-api-service
import dataiku
from dataiku.runnables import Runnable
import os
import sys
import shutil
import constants


def get_params(config, client, project):
    """
    Check the macro input parameters and return them as a dict.
    Depending on the option, we set service_id as service_id_existing or service_id_new, that's why this function is a get_params, not just check_params.
    Some checks are done to make sure the input is in the dedicated DSS object list : code-env, managed_folder, api service, endpoint.
    """
    
    params = {}
    
    model_folder_id = config.get("model_folder_id")
    list_folders = [folder.get("id") for folder in project.list_managed_folders()]
    assert model_folder_id, "Folder ID is empty"
    assert model_folder_id in list_folders, "Folder ID %s must be the id of a managed folder containing a model trained with the deeplearning-image plugin. The folder must belong to the project in which is executed the macro" % model_folder_id
    params["model_folder_id"] = model_folder_id
    
    create_new_service = config.get("create_new_service")
    assert type(create_new_service) is bool, "create_new_service is not bool: %r" % create_new_service
    params["create_new_service"] = create_new_service
    
    list_service = [service.get("id") for service in project.list_api_services()]
    if create_new_service:
        service_id = config.get("service_id_new")
        assert service_id, "Service ID is empty"
        assert service_id not in list_service, "Service ID %s already in use, find a new id or uncheck the create new service option to use an existing service" % service_id
        project.create_api_service(service_id)
    else :
        service_id = config.get("service_id_existing")
        assert service_id, "Service ID is empty"
        assert service_id in list_service, "Service ID : %s not found" % service_id
    
    params["service_id"] = service_id 
    list_endpoints = [endpoint.get("id") for endpoint in project.get_api_service(service_id).get_settings().get_raw()["endpoints"]]
    endpoint_id = config.get("endpoint_id")
    assert endpoint_id, "Endpoint ID is empty"
    if endpoint_id in list_endpoints:
        print "Will override endpoint %s" % endpoint_id
    else :
        print "Create new endpoint %s in service %s" % (endpoint_id, service_id)
    params["endpoint_id"] = config.get("endpoint_id")
    #TO-DO custom html select to get the list of endpoints
    

    """
    create_package = config.get("create_package")
    assert type(create_new_service) is bool, "create_package is not bool: %r" % create_package
    params["create_package"] = create_package

    if create_package :
        list_packages = [package.get("id") for package in project.get_api_service(service_id).list_packages()]
        package_id = config.get("package_id")
        assert package_id, "Package ID is empty"
        assert package_id not in list_packages, "Package ID already in use"
        params["package_id"] = package_id
    #TO-DO custom html select to get the list of API service packages
    """
            
    max_nb_labels = config.get("max_nb_labels")
    assert max_nb_labels, "Max number of labels is empty"
    assert type(max_nb_labels) is int, "Max number of labels is not an int : %s "%type(max_nb_labels)
    assert max_nb_labels > 0 , "Max number of labels must be strictly greater than 0"
    params["max_nb_labels"] = max_nb_labels
    
    min_threshold = config.get("min_threshold")
    assert min_threshold, "Min threshold is empty"
    assert type(min_threshold) is float, "Min threshold is not a float : %s "%type(min_threshold)
    assert (min_threshold >= 0) and (min_threshold <= 1)  , "Min threshold must be between 0 and 1"
    params["min_threshold"] = min_threshold
        
    env_name = 'plugin_deeplearning-image-cpu_api_node'
    params['code_env_name'] = env_name

    return params

def copy_plugin_to_dss_folder(plugin_id, folder_id, project_key, force_copy=False):    
    """
    Copy python-lib from a plugin to a managed folder
    """
    
    root_path = dataiku.get_custom_variables(project_key=project_key)['dip.home']
    plugin_lib_path = os.path.join(root_path, 'plugins', 'installed', plugin_id, 'python-lib') # TODO change this to plugins/installed/...
     
    folder_path = dataiku.Folder(folder_id, project_key=project_key).get_path()
    lib_folder_path = os.path.join(folder_path, 'python-lib')
    
    if os.path.isdir(lib_folder_path) and force_copy:
        shutil.rmtree(lib_folder_path)
    
    if not os.path.isdir(lib_folder_path):
        os.mkdir(lib_folder_path)
        sys.path.append(lib_folder_path)

        for item in os.listdir(plugin_lib_path):
            s = os.path.join(plugin_lib_path, item)
            d = os.path.join(lib_folder_path, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, symlinks=False, ignore=None)
            else:
                shutil.copy2(s, d)
    else:
        print('python-lib already exists in folder')


def get_api_service(params, project):
    """
    Create or get an api service dss object and return it
    """
    
    if params.get('create_new_service') :
        api_service = project.create_api_service(params.get("service_id"))    
    else :
        api_service = project.get_api_service(params.get("service_id"))
    return api_service


def create_api_code_env(client, env_name):
    
    already_exist = env_name in [env.get('envName') for env in client.list_code_envs()]
    
    if not already_exist:
        _ = client.create_code_env(env_lang='PYTHON', env_name = env_name, deployment_mode = 'DESIGN_MANAGED')

    my_env = client.get_code_env('PYTHON', env_name)
    env_def = my_env.get_definition()
    env_def['specPackageList'] = 'boto3\nscipy'
    env_def['specPackageList'] = 'scikit-learn==0.19\ntensorflow==1.4.0\nkeras==2.1.2\nh5py>=2.7.1\nPillow\npip==9.0.1'
    env_def['desc']['installCorePackages'] = True
    my_env.set_definition(env_def)
    my_env.update_packages()


def get_model_endpoint_settings(params):
    """
    Create a endpoints dict that will be added to a list of endpoints of an api service
    """
    
    endpoint_settings = dict()
    endpoint_settings["id"] = params.get("endpoint_id")
    endpoint_settings["type"] = "PY_FUNCTION"
    endpoint_settings["userFunctionName"] = "api_py_function"

    code_env = {u'envMode': u'EXPLICIT_ENV', u'envName': params.get('code_env_name')} 
    endpoint_settings['envSelection'] = code_env


    folder_list = []
    folder_list.append({"ref": params.get("model_folder_id")})
    endpoint_settings["inputFolderRefs"] = folder_list

    test_query = {}
    input_query = {"img_b64": """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"""}
    test_query["name"] = "Score car image"
    test_query["q"] = input_query
    list_queries = list()
    list_queries.append(test_query)
    endpoint_settings["testQueries"] = list_queries

    endpoint_settings["code"] = """import dataiku
import pandas as pd
from dataiku.customrecipe import *
import numpy as np
import json
import os
import glob
from io import BytesIO
from ast import literal_eval
import base64
import sys

model_folder_path = folders[0]

#Load plugin libs
sys.path.append(os.path.join(model_folder_path, "python-lib"))
import dl_image_toolbox_utils as utils
import constants

max_nb_labels = %s
min_threshold = %s


# Model
model_and_pp = utils.load_instantiate_keras_model_preprocessing(model_folder_path, goal=constants.SCORING)
model = model_and_pp["model"]
preprocessing = model_and_pp["preprocessing"]
model_input_shape = utils.get_model_input_shape(model, model_folder_path)

# (classId -> Name) mapping
labels_df = None
labels_path = utils.get_file_path(model_folder_path, constants.MODEL_LABELS_FILE)
if os.path.isfile(labels_path):
    labels_df = pd.read_csv(labels_path, sep=",")
    labels_df = labels_df.set_index('id')
else:
    print("------ Info: No csv file in the model folder, will not use class names. ------")


def api_py_function(img_b64):
    #takes in input the image encoded as base64 base64.b64encode(open(img_path, "rb").read())
    #preprocess the image and score it

    print("------ Info: Start loading image ------")
    img_b64_decode = base64.b64decode(img_b64)
    img = BytesIO(img_b64_decode)
    print("------ Info: Finished loading image ------")

    print("------ Info: Start preprocessing image ------")
    preprocessed_img = utils.preprocess_img(img, model_input_shape, preprocessing)
    print("------ Info: Finished preprocessing image ------")
    batch_im = np.expand_dims(preprocessed_img, 0)

    print("------ Info: Start predicting ------")
    prediction_batch = utils.get_predictions(model, batch_im, max_nb_labels, min_threshold, labels_df)
    print("------ Info: Finished predicting ------")

    return literal_eval(prediction_batch[0])"""%(params.get("max_nb_labels"), params.get("min_threshold"))

    return endpoint_settings
     
        
def create_python_endpoint(api_service, setting_dict):
    """
    Create or update an endpoint to the API service DSS object api_service.
    """
    
    api_setting = api_service.get_settings()
    api_setting_details = api_setting.get_raw()
    
    
    new_endpoints = []
    for endpoint in api_setting_details['endpoints']:
        if endpoint.get('id') != setting_dict.get('id'):
            new_endpoints.append(endpoint)
    
    new_endpoints.append(setting_dict)
    
    api_setting_details['endpoints'] = new_endpoints
    api_setting.save()
    

def get_html_result(params):
    """
    Get the result html string of the macro
    """
    
    html_str = """<div> Model succesfully deployed to API designer </div>
<div>Model folder : %s</div>
<div>API service : %s</div>
<div>Endpoint : %s</div>
<a href="https://www.w3schools.com">See Service in API designer</a>
"""%(params.get('model_folder_id'), params.get('service_id'), params.get('endpoint_id'))
    
    return html_str

    

class MyRunnable(Runnable):
    """The base interface for a Python runnable"""

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        :param client: DSS client
        :param project: DSS project in which the macro is executed
        :param plugin_id: name of the plugin in use
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()
        self.project = self.client.get_project(self.project_key)
        self.plugin_id = "deeplearning-image-cpu"
        #TO-DO way of getting the plugin_id within the macro? plugin_config seems empty

        
    def get_progress_target(self):
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """
        return None
    

    def run(self, progress_callback):
        """
        Do stuff here. Can return a string or raise an exception.
        The progress_callback is a function expecting 1 value: current progress
        """
        params = get_params(self.config, self.client, self.project)
        copy_plugin_to_dss_folder(self.plugin_id, params.get("model_folder_id"), self.project_key)
        create_api_code_env(self.client, params.get('code_env_name'))
        api_service = get_api_service(params, self.project)            
        endpoint_settings = get_model_endpoint_settings(params)
        create_python_endpoint(api_service, endpoint_settings)
        
        if params.get('create_package'):
            api_service.create_package(params.get('package_id'))
            
        html_str = get_html_result(params)
        
        return html_str
            


        
        