#############################
# Your original recipe
#############################

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
import os
import sys
import random
import math
import numpy as np
import skimage.io
import matplotlib
import matplotlib.pyplot as plt
import itertools
import colorsys
from skimage.measure import find_contours
from matplotlib import patches,  lines
from matplotlib.patches import Polygon

# Checking whether pycocotools is installed
try :
    import pycocotools
except : 
    print("##### WARNING ##### Couldn't find pycocotools in installed packages : will proceed to installing the package in plugin codenv")
    client = dataiku.api_client()
    codenv = client.get_code_env('python','plugin_image-segmentation_managed')
    codenv_def = condev.get_definition()
    codenv_def['specPackageList'] = 'git+https://github.com/waleedka/coco.git#subdirectory=PythonAPI'
    codenv.set_definition(codenv_def)
    
    print('#'*50)
    print('Start updating codenv plugin_image-segmentation_managed with git+https://github.com/waleedka/coco.git#subdirectory=PythonAPI')
    codenv.update_packages()
    print ('Codenv updated')
# Import Mask RCNN
from mrcnn import utils
import mrcnn.model as modellib
from mrcnn import visualize
# Import COCO config
import coco
from coco import coco

# Directory to save logs and trained model
#MODEL_DIR = os.path.join(ROOT_DIR, "logs")

# Local path to trained weights file
model_folder_input = get_input_names_for_role('model_folder')[0]
model_folder_path = dataiku.Folder(model_folder_input).get_path()
COCO_MODEL_PATH = os.path.join(model_folder_path, "mask_rcnn_coco.h5")
# Download COCO trained weights from Releases if needed

#Directory of images to run detection on
image_folder_input = get_input_names_for_role('image_folder')[0]
image_folder = dataiku.Folder(image_folder_input)
IMAGE_DIR = image_folder.get_path()

image_folder_output = get_output_names_for_role('scored_folder')[0]
images_segmented = dataiku.Folder(image_folder_output)
SEGMENTED_IM_DIR = images_segmented.get_path()

details_folder_name = get_output_names_for_role('details_folder')[0]
details_folder = dataiku.Folder(details_folder_name)
DETAILS_DIR = details_folder.get_path()
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
class InferenceConfig(coco.CocoConfig):
    # Set batch size to 1 since we'll be running inference on
    # one image at a time. Batch size = GPU_COUNT * IMAGES_PER_GPU
    GPU_COUNT = 1
    IMAGES_PER_GPU = len(image_folder.list_paths_in_partition(partition=''))

config = InferenceConfig()

model = modellib.MaskRCNN(mode="inference", model_dir=model_folder_path, config=config)
model.load_weights(COCO_MODEL_PATH, by_name=True)


class_names = ['BG', 'person', 'bicycle', 'car', 'motorcycle', 'airplane',
               'bus', 'train', 'truck', 'boat', 'traffic light',
               'fire hydrant', 'stop sign', 'parking meter', 'bench', 'bird',
               'cat', 'dog', 'horse', 'sheep', 'cow', 'elephant', 'bear',
               'zebra', 'giraffe', 'backpack', 'umbrella', 'handbag', 'tie',
               'suitcase', 'frisbee', 'skis', 'snowboard', 'sports ball',
               'kite', 'baseball bat', 'baseball glove', 'skateboard',
               'surfboard', 'tennis racket', 'bottle', 'wine glass', 'cup',
               'fork', 'knife', 'spoon', 'bowl', 'banana', 'apple',
               'sandwich', 'orange', 'broccoli', 'carrot', 'hot dog', 'pizza',
               'donut', 'cake', 'chair', 'couch', 'potted plant', 'bed',
               'dining table', 'toilet', 'tv', 'laptop', 'mouse', 'remote',
               'keyboard', 'cell phone', 'microwave', 'oven', 'toaster',
               'sink', 'refrigerator', 'book', 'clock', 'vase', 'scissors',
               'teddy bear', 'hair drier', 'toothbrush']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
to_score = image_folder.list_paths_in_partition(partition='')
image_list=[]
for im in to_score :
    image_path = IMAGE_DIR+im
    image = skimage.io.imread(image_path)
    image_list.append(image)

result=model.detect(image_list, verbose=1)

for i, im in enumerate(result):
    result[i]["image_path"] = to_score[i]
result_json=[]

def jsonify(data):
    json_data = dict()
    for key, value in data.items():
        if isinstance(value, list): # for lists
            value = [ jsonify(item) if isinstance(item, dict) else item for item in value ]
        if isinstance(value, dict): # for nested lists
            value = jsonify(value)
        if isinstance(key, int): # if key is integer: > to string
            key = str(key)
        if type(value).__module__=='numpy': # if value is numpy.*: > to python list
            value = value.tolist()
        json_data[key] = value
    return json_data

for res in result:
    result_json.append(jsonify(res))

with open(DETAILS_DIR + '/results.json', 'w') as outfile:
    json.dump(result_json, outfile)

output_images = get_recipe_config().get('save_image')
if output_images : 
    for ix, im_path in enumerate(to_score):
        r = result[ix]
        image= image_list[ix]
        im_name = im_path[:-4]+'_scored.png'
        visualize.save_instance(image, r['rois'], r['masks'], r['class_ids'],
                         class_names, r['scores'], path=SEGMENTED_IM_DIR, name=im_name
                     )