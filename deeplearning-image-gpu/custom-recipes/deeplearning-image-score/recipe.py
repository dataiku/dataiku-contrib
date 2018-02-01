import dataiku
import pandas as pd
from dataiku.customrecipe import *
import numpy as np
import json
import os
import glob
import constants
import dl_image_toolbox_utils as utils


###################################################################################################################
## LOADING ALL REQUIRED INFO AND 
##      SETTING VARIABLES
###################################################################################################################

# Config
recipe_config = get_recipe_config()
max_nb_labels = int(recipe_config['max_nb_labels'])
min_threshold = float(recipe_config['min_threshold'])
should_use_gpu = recipe_config.get('should_use_gpu', False)

# gpu
utils.load_gpu_options(should_use_gpu, recipe_config['list_gpu'], recipe_config['gpu_allocation'])

# Plugin parameters
image_folder_input_name = get_input_names_for_role('image_folder')[0]
image_folder = dataiku.Folder(image_folder_input_name)
utils.check_managed_folder_filesystem(image_folder)
image_folder_path = image_folder.get_path()

model_folder_input_name = get_input_names_for_role('model_folder')[0]
model_folder = dataiku.Folder(model_folder_input_name)
utils.check_managed_folder_filesystem(model_folder)
model_folder_path = model_folder.get_path()

output_name = get_output_names_for_role('scored_dataset')[0]
output_dataset =  dataiku.Dataset(output_name)

# Model
model_and_pp = utils.load_instantiate_keras_model_preprocessing(model_folder_path, goal=constants.SCORING)
model = model_and_pp["model"]
preprocessing = model_and_pp["preprocessing"]
model_input_shape = model.input_shape[1:3]

# (classId -> Name) mapping
labels_df = None
labels_path = os.path.join(model_folder_path, constants.MODEL_LABELS_FILE)
if os.path.isfile(labels_path):
    labels_df = pd.read_csv(labels_path, sep=",")
    labels_df = labels_df.set_index('id')
else:
    print("------ \n Info: No csv file in the model folder, will not use class names. \n ------")

# Image paths
images_paths = os.listdir(image_folder_path)

###################################################################################################################
## COMPUTING SCORE
###################################################################################################################

# Helper for predicting
def predict(limit = 5, min_threshold = 0):
    batch_size = 100
    n = 0
    results = {"prediction": [], "error": []}
    num_images = len(images_paths)
    while True:
        if (n * batch_size) >= num_images:
            break

        next_batch_list = []
        error_indices = []
        for index_in_batch, i in enumerate(range(n*batch_size, min((n+1)*batch_size, num_images))):
            img_path = images_paths[i]
            try:
                preprocessed_img = utils.preprocess_img(os.path.join(image_folder_path, img_path), model_input_shape, preprocessing)
                next_batch_list.append(preprocessed_img)
            except IOError as e:
                print("Cannot read the image '{}', skipping it. Error: {}".format(img_path, e))
                error_indices.append(index_in_batch)
        next_batch = np.array(next_batch_list)

        prediction_batch = utils.get_predictions(model, next_batch, limit, min_threshold, labels_df)
        error_batch = [0] * len(prediction_batch)

        for err_index in error_indices:
            prediction_batch.insert(err_index, None)
            error_batch.insert(err_index, 1)
        
        results["prediction"].extend(prediction_batch)
        results["error"].extend(error_batch)
        n+=1
        print("{} images treated, out of {}".format(min(n * batch_size, num_images), num_images))
    return results

# Make the predictions
print("------ \n Info: Start predicting \n ------")
predictions = predict(max_nb_labels, min_threshold)
print("------ \n Info: Finished predicting \n ------")

###################################################################################################################
## SAVING RESULTS
###################################################################################################################

# Prepare results
output = pd.DataFrame()
output["images"] = images_paths
print("------->" + str(output))
output["prediction"] = predictions["prediction"]
output["error"] = predictions["error"]

# Write to output dataset    
print("------ \n Info: Writing to output dataset \n ------")
output_dataset.write_with_schema(pd.DataFrame(output))
print("------ \n Info: END of recipe \n ------")