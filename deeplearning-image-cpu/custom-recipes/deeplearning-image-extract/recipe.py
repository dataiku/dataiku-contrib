import dataiku
import pandas as pd
from dataiku.customrecipe import *
from keras.models import load_model, Model
import numpy as np
import json
import os
import glob
import dl_image_toolbox_utils as utils
import constants
os.environ["CUDA_VISIBLE_DEVICES"] = ""

###################################################################################################################
## LOADING ALL REQUIRED INFO AND 
##      SETTING VARIABLES
###################################################################################################################

recipe_config = get_recipe_config()
extract_layer_index = int(recipe_config["extract_layer_index"])
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

output_name = get_output_names_for_role('feature_dataset')[0]
output_dataset =  dataiku.Dataset(output_name)



# Model
model_and_pp = utils.load_instantiate_keras_model_preprocessing(model_folder_path, goal=constants.SCORING)
model = model_and_pp["model"]
preprocessing = model_and_pp["preprocessing"]

model = Model(inputs=model.input, outputs=model.layers[extract_layer_index].output)
model_input_shape = utils.get_model_input_shape(model, model_folder_path)

# Image paths
images_paths = os.listdir(image_folder_path)

###################################################################################################################
## EXTRACTING FEATURES
###################################################################################################################

# Helper for predicting
def get_predictions():
    batch_size = 100
    n = 0
    results = {"prediction": [], "error": []}
    num_images = len(images_paths)
    while True:
        if (n * batch_size) >= num_images:
            break

        next_batch_list = []
        error_indices = []
        for index_in_batch, i in enumerate(range(n*batch_size, min((n + 1)*batch_size, num_images))):
            img_path = images_paths[i]
            try:
                preprocssed_img = utils.preprocess_img(utils.get_file_path(image_folder_path, img_path), model_input_shape, preprocessing)
                next_batch_list.append(preprocssed_img)
            except IOError as e:
                print("Cannot read the image '{}', skipping it. Error: {}".format(img_path, e))
                error_indices.append(index_in_batch)
        next_batch = np.array(next_batch_list)

        prediction_batch = model.predict(next_batch).tolist()
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
predictions = get_predictions()
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