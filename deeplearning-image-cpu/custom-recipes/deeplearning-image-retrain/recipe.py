from dataiku.customrecipe import *
import dl_image_toolbox_utils as utils
from sklearn.model_selection import train_test_split
from keras import optimizers, initializers, metrics, regularizers
from keras.callbacks import ModelCheckpoint, TensorBoard
from keras.layers import Dropout, Dense
from keras.models import Model
from keras.utils.training_utils import multi_gpu_model
from keras.preprocessing.image import ImageDataGenerator
import tensorflow as tf
import pandas as pd
import constants
import math
import shutil
import numpy as np
import dataiku

###################################################################################################################
## LOADING ALL REQUIRED INFO AND 
##      SETTING VARIABLES
###################################################################################################################

# Recipe config
recipe_config = get_recipe_config()
should_use_gpu = recipe_config.get('should_use_gpu', False)
list_gpu = recipe_config["list_gpu"]
gpu_allocation = recipe_config["gpu_allocation"]
train_ratio = float(recipe_config["train_ratio"])
input_shape = (int(recipe_config["image_width"]),int(recipe_config["image_height"]),3)
batch_size = int(recipe_config["batch_size"])
optimizer = recipe_config["model_optimizer"]
learning_rate = recipe_config["model_learning_rate"]
custom_params_opti = recipe_config.get("model_custom_params_opti", [])
nb_epochs = int(recipe_config["nb_epochs"])
nb_steps_per_epoch = int(recipe_config["nb_steps_per_epoch"])
nb_validation_steps = int(recipe_config["nb_validation_steps"])
data_augmentation = recipe_config["data_augmentation"]
n_augmentation = int(recipe_config["n_augmentation"])
custom_params_data_augment = recipe_config.get("model_custom_params_data_augmentation", [])
tensorboard = recipe_config["tensorboard"]
random_seed = int(recipe_config["random_seed"])

# gpu
gpu_options = utils.load_gpu_options(should_use_gpu, list_gpu, gpu_allocation)
n_gpu = gpu_options.get("n_gpu", 0)

# Folders
image_folder_input_name = get_input_names_for_role('image_folder')[0]
image_folder = dataiku.Folder(image_folder_input_name)
utils.check_managed_folder_filesystem(image_folder)
image_folder_path = image_folder.get_path()

model_folder_input_name = get_input_names_for_role('model_folder')[0]
model_folder = dataiku.Folder(model_folder_input_name)
utils.check_managed_folder_filesystem(model_folder)
model_folder_path = model_folder.get_path()

output_model_folder_name = get_output_names_for_role('model_output')[0]
output_model_folder = dataiku.Folder(output_model_folder_name)
utils.check_managed_folder_filesystem(output_model_folder)
output_model_folder_path = output_model_folder.get_path()

# Label Dataset : Keeping only the two relevant columns
label_dataset_input_name = get_input_names_for_role('label_dataset')[0]
label_dataset = dataiku.Dataset(label_dataset_input_name)
renaming_mapping = {
        recipe_config["col_filename"]: constants.FILENAME,
        recipe_config["col_label"]: constants.LABEL
}
label_df = label_dataset.get_dataframe().rename(columns=renaming_mapping)[renaming_mapping.values()]

# Model config
model_config = utils.get_config(model_folder_path)

###################################################################################################################
## BUILD TRAIN/TEST SETS
###################################################################################################################

df_train, df_test = train_test_split(label_df, stratify=label_df[constants.LABEL], train_size=train_ratio, random_state=random_seed)
labels = list(np.unique(label_df[constants.LABEL]))
n_classes = len(labels)


###################################################################################################################
## LOAD MODEL
###################################################################################################################

# Loading pre-trained model
def load_model_and_apply_recipe_params(model_folder_path, input_shape, n_classes, recipe_config):

    pooling = recipe_config["model_pooling"]
    reg = recipe_config["model_reg"]
    dropout = float(recipe_config["model_dropout"])

    model_and_pp = utils.load_instantiate_keras_model_preprocessing(model_folder_path, goal=constants.RETRAINING, 
                                                                    input_shape=input_shape,
                                                                    pooling=pooling,
                                                                    reg=reg,
                                                                    dropout=dropout,
                                                                    n_classes=n_classes)

    model = model_and_pp["model"]
    preprocessing = model_and_pp["preprocessing"]
    model_params = model_and_pp["model_params"]

    # CHOOSING LAYER TO RETRAIN
    layer_to_retrain = recipe_config["layer_to_retrain"]
    print("Will Retrain layer(s) with mode: {}".format(layer_to_retrain))
    if layer_to_retrain == "all" :
        for lay in model.layers :
            lay.trainable = True
            
    elif layer_to_retrain == "last" :
        for lay in model.layers[:-1] :
            lay.trainable = False
        lay = model.layers[-1]
        lay.trainable = True
        
    elif layer_to_retrain == "n_last" :
        n_last = int(recipe_config["layer_to_retrain_n"])
        for lay in model.layers[:-n_last] :
            lay.trainable = False
        for lay in model.layers[-n_last:] :
            lay.trainable = True

    model.summary()

    return model, preprocessing, model_params

if should_use_gpu and n_gpu > 1:
    with tf.device('/cpu:0'):
        base_model, preprocessing, model_params = load_model_and_apply_recipe_params(model_folder_path, input_shape, n_classes, recipe_config)
    model = multi_gpu_model(base_model, n_gpu)
else:
    model, preprocessing, model_params = load_model_and_apply_recipe_params(model_folder_path, input_shape, n_classes, recipe_config)

###################################################################################################################
## BUILD GENERATORS
## Info: Generators must loop infinitely, each loop yielding the batches of preprocessed data.
##       It will be used at each epoch, hence the infinite loop.
###################################################################################################################


@utils.threadsafe_generator
def augmentation_generator(df_imgs, image_folder_path, batch_size, n_augmentation, input_shape, labels, preprocessing, TrainImageGen):

    nb_imgs = df_imgs.shape[0]
    batch_size_adapted = int( batch_size / n_augmentation )
    nb_batch = int(math.ceil( nb_imgs * 1.0 / batch_size_adapted ))

    while True:

        for num_batch in range(nb_batch):

            df_imgs_batch = df_imgs.iloc[num_batch * batch_size_adapted : (num_batch + 1) * batch_size_adapted, :]
            nb_imgs_batch = df_imgs_batch.shape[0]

            X_batch_list = []
            y_batch_list = []

            for num_img in range(nb_imgs_batch):

                row = df_imgs_batch.iloc[num_img, :]
                img_filename = row[constants.FILENAME]
                img_path = utils.get_file_path(image_folder_path, img_filename)
                label = row[constants.LABEL]
                label_index = labels.index(label)
                
                try: 
                    x = utils.preprocess_img(img_path, input_shape, preprocessing)
                    x = np.tile(x, (n_augmentation, 1, 1, 1))

                    # TrainImageGen returns infinite loop, each of which yields batch data
                    for batch in TrainImageGen.flow(x, batch_size=n_augmentation):
                        X_batch_list.append(batch)
                        y_batch_list.extend([label_index] * n_augmentation)
                        break
                except IOError as e:
                    print("Cannot read the image '{}', skipping it. Error: {}".format(img_filename, e))

            X_batch = np.concatenate(X_batch_list)

            actual_batch_size = X_batch.shape[0]
            y_batch = np.zeros((actual_batch_size, n_classes))
            y_batch[range(actual_batch_size), y_batch_list] = 1

            yield(X_batch, y_batch)


@utils.threadsafe_generator
def no_augmentation_generator(df_imgs, image_folder_path, batch_size, input_shape, labels, preprocessing):

    nb_imgs = df_imgs.shape[0]
    nb_batch = int(math.ceil( nb_imgs * 1.0 / batch_size))

    while True:

        for num_batch in range(nb_batch):

            df_imgs_batch = df_imgs.iloc[num_batch * batch_size : (num_batch + 1) * batch_size, :]
            nb_imgs_batch = df_imgs_batch.shape[0]
            X_batch_list = []
            y_batch_list = []

            for num_img in range(nb_imgs_batch):

                row = df_imgs_batch.iloc[num_img, :]
                img_filename = row[constants.FILENAME]

                img_path = utils.get_file_path(image_folder_path, img_filename)
                label = row[constants.LABEL]
                label_index = labels.index(label)
                try :
                    x = utils.preprocess_img(img_path, input_shape, preprocessing)
                    X_batch_list.append(x)
                    y_batch_list.append(label_index)
                except IOError as e:
                    print("Cannot read the image '{}', skipping it. Error: {}".format(img_filename, e))

            X_batch = np.array(X_batch_list)
            
            actual_batch_size = X_batch.shape[0]
            y_batch = np.zeros((actual_batch_size, n_classes))
            y_batch[range(actual_batch_size), y_batch_list] = 1

            yield(X_batch,y_batch)


if data_augmentation:
    print("Using data augmentation with {} images generated per training image\n".format(n_augmentation))
    params_data_augment = utils.clean_custom_params(custom_params_data_augment, params_type="Data Augmentation")
    TrainImageGen = ImageDataGenerator(**params_data_augment)
    train_generator = augmentation_generator(df_train, image_folder_path, batch_size, n_augmentation, input_shape, labels, preprocessing, TrainImageGen)
else:
    train_generator = no_augmentation_generator(df_train, image_folder_path, batch_size, input_shape, labels, preprocessing)

test_generator = no_augmentation_generator(df_test, image_folder_path, batch_size, input_shape, labels, preprocessing)

###################################################################################################################
## COMPILE MODEL
###################################################################################################################


if optimizer == "adam":
    model_opti_class = optimizers.Adam
elif optimizer == "adagrad":
    model_opti_class = optimizers.Adagrad
elif optimizer == "sgd":
    model_opti_class = optimizers.SGD

# Cleaning custom parameters
params_opti = utils.clean_custom_params(custom_params_opti)
params_opti["lr"] = learning_rate

model_opti = model_opti_class(**params_opti)
model.compile(optimizer=model_opti, loss='categorical_crossentropy',metrics=['accuracy'])

callback_list = []

###################################################################################################################
## BUILD MODEL CHECKPOINT
###################################################################################################################

model_weights_path = utils.get_weights_path(output_model_folder_path, model_config, suffix=constants.RETRAINED_SUFFIX, should_exist=False)
should_save_weights_only = utils.should_save_weights_only(model_config)

if should_use_gpu and n_gpu > 1:
    mcheck = utils.MultiGPUModelCheckpoint(model_weights_path, base_model, monitor="val_loss", save_best_only=True, save_weights_only=should_save_weights_only)
else:
    mcheck = ModelCheckpoint(model_weights_path, monitor="val_loss", save_best_only=True, save_weights_only=should_save_weights_only)

callback_list.append(mcheck)

###################################################################################################################
## TENSORBOARD
###################################################################################################################

if tensorboard:
    log_path = utils.get_file_path(output_model_folder_path, constants.TENSORBOARD_LOGS)

    # If already folder at loger_path, delete it
    if os.path.isdir(log_path):
        shutil.rmtree(log_path)

    tsboard = TensorBoard(log_dir=log_path, write_graph=True)
    callback_list.append(tsboard)

###################################################################################################################
## TRAIN MODEL
###################################################################################################################


model.fit_generator(
    train_generator,
    steps_per_epoch=nb_steps_per_epoch,
    epochs=nb_epochs,
    validation_data=test_generator,
    validation_steps=nb_validation_steps,
    callbacks=callback_list,
    shuffle=False,
    verbose=2)

###################################################################################################################
## SAVING NEW CONFIG AND LABELS
###################################################################################################################

model_config[constants.RETRAINED] = True
model_config[constants.TOP_PARAMS] = model_params
utils.write_config(output_model_folder_path, model_config)

df_labels = pd.DataFrame({"id": range(n_classes), "className": labels})
df_labels.to_csv(utils.get_file_path(output_model_folder_path, constants.MODEL_LABELS_FILE), index=False)

# Computing model info
utils.save_model_info(output_model_folder_path)
