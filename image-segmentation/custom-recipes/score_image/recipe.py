# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *
import os
import json
import skimage.io
import logging
from mrcnn import utils, visualize
import mrcnn.model as modellib
from coco import coco
from utils import jsonify

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,  # avoid getting log from 3rd party module
                    format='image-segmentation plugin %(levelname)s - %(message)s')

# Checking whether pycocotools is installed, if not importing it
try :
    import pycocotools
except : 
    raise ValueError("""The Codenv update failed : could not import pycocotools,
                    please run download-weights macro to update the codenv""")


##### Recipe Inputs #####
plugin_config = get_recipe_config()
logger.info(plugin_config)

# Local path to trained weights file
model_folder_input = get_input_names_for_role('model_folder')[0]
model_folder_path = dataiku.Folder(model_folder_input).get_path()
COCO_MODEL_PATH = os.path.join(model_folder_path,
                               "mask_rcnn_coco.h5")

image_folder_input = get_input_names_for_role('image_folder')[0]
image_folder = dataiku.Folder(image_folder_input)
IMAGE_DIR = image_folder.get_path()
to_score = image_folder.list_paths_in_partition(partition='')


image_folder_output = get_output_names_for_role('scored_folder')[0]
images_segmented = dataiku.Folder(image_folder_output)
SEGMENTED_IM_DIR = images_segmented.get_path()

details_folder_name = get_output_names_for_role('details_folder')[0]
details_folder = dataiku.Folder(details_folder_name)
DETAILS_DIR = details_folder.get_path()

output_images = get_recipe_config().get('save_image')

#### Recipe body ####

class InferenceConfig(coco.CocoConfig):
    # Set batch size to 1 since we'll be running inference on
    # one image at a time. Batch size = GPU_COUNT * IMAGES_PER_GPU
    GPU_COUNT = 1
    IMAGES_PER_GPU = len(image_folder.list_paths_in_partition(partition=''))

config = InferenceConfig()

# Loading pre-trained model with weights
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

image_list = []
for im in to_score :
    image_path = IMAGE_DIR+im
    image = skimage.io.imread(image_path)
    image_list.append(image)

result = model.detect(image_list,verbose=1)

for i, im in enumerate(result):
    result[i]["image_path"] = to_score[i]
result_json = []


for res in result:
    result_json.append(jsonify(res))

#### Writing output to folders #####
with open(DETAILS_DIR + '/results.json', 'w') as outfile:
    json.dump(result_json, 
              outfile)


if output_images : 
    for ix, im_path in enumerate(to_score):
        r = result[ix]
        image = image_list[ix]
        im_name = im_path[:-4] + '_scored.png'
        visualize.save_instance(image, r['rois'],
                                r['masks'], r['class_ids'],
                                class_names, r['scores'], 
                                path=SEGMENTED_IM_DIR, name=im_name
                               )