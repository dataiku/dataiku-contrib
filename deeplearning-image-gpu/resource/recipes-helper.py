import dataiku
import glob
import pandas as pd
import dl_image_toolbox_utils as utils
import tensorflow as tf
import constants
import os

# We deactivate GPU for this script, because all the methods only need to 
# fetch information about model and do not make computation
utils.deactivate_gpu()

def do(payload, config, plugin_config, inputs):
    if "method" not in payload:
        return {}

    client = dataiku.api_client()

    if payload["method"] == "get-info-scoring":
        return get_info_scoring(inputs)

    if payload["method"] == "get-info-about-model":
        return get_info_about_model(inputs)

    if payload["method"] == "get-info-retrain":
        return get_info_retrain(inputs)

def get_info_scoring(inputs):
    return add_can_use_gpu_to_resp({})

def get_info_about_model(inputs):
    model_folder = get_model_folder_path(inputs)

    model_info = utils.get_model_info(model_folder, goal=constants.SCORING)
    config = utils.get_config(model_folder)

    return  add_can_use_gpu_to_resp({
        "layers": model_info["layers"],
        "summary": model_info["summary"],
        "default_layer_index": config["extract_layer_default_index"]
    })

def get_info_retrain(inputs):
    model_folder = get_model_folder_path(inputs)

    model_info = utils.get_model_info(model_folder, goal=constants.BEFORE_TRAIN)

    label_dataset = get_label_dataset(inputs)
    columns = [c["name"] for c in label_dataset.read_schema()]

    model_config = utils.get_config(model_folder)

    return add_can_use_gpu_to_resp({"summary": model_info["summary"], "columns": columns, "model_config": model_config})

def get_model_folder_path(inputs):
    # Retrieving model folder
    model_folder_full_name = get_input_name_from_role(inputs, "model_folder")
    model_folder = dataiku.Folder(model_folder_full_name).get_path()

    return model_folder

def get_label_dataset(inputs):
    label_dataset_full_name = get_input_name_from_role(inputs, "label_dataset")
    label_dataset = dataiku.Dataset(label_dataset_full_name)
    return label_dataset

def get_input_name_from_role(inputs, role):
    return [inp for inp in inputs if inp["role"] == role][0]["fullName"]

def add_can_use_gpu_to_resp(response):
    response["can_use_gpu"] = utils.can_use_gpu()
    return response
