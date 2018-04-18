import os
from keras.preprocessing.image import img_to_array, load_img
from keras.layers import GlobalAveragePooling2D, GlobalMaxPooling2D, Flatten, Dropout, Dense
from keras.models import Model
from keras.callbacks import ModelCheckpoint
from keras import regularizers
import constants
import threading
import json
from collections import OrderedDict
import StringIO
import numpy as np
import tensorflow as tf
import sys

# Support Truncated Images with PIL
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

###################################################################################################################
## MODELS LIST
###################################################################################################################

from keras.applications.resnet50 import ResNet50, preprocess_input as resnet50_preprocessing
from keras.applications.xception import Xception, preprocess_input as xception_preprocessing
from keras.applications.inception_v3 import InceptionV3, preprocess_input as inceptionv3_preprocessing
from keras.applications.vgg16 import VGG16, preprocess_input as vgg16_preprocessing


# INFO : when adding a new architecture, you must add a select-option in python-runnables/dl-toolbox-download-models/runnable.json
#        with the label architecture_trainedon to make it available, along with new a constant in python-lib/constants.py
keras_applications = {
    constants.RESNET: {
        "model_func": ResNet50, 
        "preprocessing": resnet50_preprocessing,
        "input_shape": (224, 224),
        "weights": {
            constants.IMAGENET: {
                "top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.2/resnet50_weights_tf_dim_ordering_tf_kernels.h5",
                "no_top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.2/resnet50_weights_tf_dim_ordering_tf_kernels_notop.h5"
            }
        }
    },
    constants.XCEPTION: {
        "model_func": Xception,
        "preprocessing": xception_preprocessing,
        "input_shape": (299, 299),
        "weights": {
            constants.IMAGENET: {
                "top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.4/xception_weights_tf_dim_ordering_tf_kernels.h5",
                "no_top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.4/xception_weights_tf_dim_ordering_tf_kernels_notop.h5",
            }
        }
    },
    constants.INCEPTIONV3: {
        "model_func": InceptionV3,
        "preprocessing": inceptionv3_preprocessing,
        "input_shape": (299, 299),
        "weights": {
            constants.IMAGENET: {
                "top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.5/inception_v3_weights_tf_dim_ordering_tf_kernels.h5",
                "no_top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.5/inception_v3_weights_tf_dim_ordering_tf_kernels_notop.h5"
            }
        }
    },
    constants.VGG16: {
        "model_func": VGG16,
        "preprocessing": vgg16_preprocessing,
        "input_shape": (224, 224),
        "weights": {
            constants.IMAGENET: {
                "top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.1/vgg16_weights_tf_dim_ordering_tf_kernels.h5",
                "no_top": "https://github.com/fchollet/deep-learning-models/releases/download/v0.1/vgg16_weights_tf_dim_ordering_tf_kernels_notop.h5"
            }
        }
    }
}

def is_keras_application(architecture):
    return architecture in keras_applications.keys()

def get_extract_layer_index(architecture, trained_on):
    # May be more complicated as the list of models grows
    return -2


def get_weights_urls(architecture, trained_on):
    if is_keras_application(architecture):
        return keras_applications[architecture]["weights"][trained_on]
    else:
        return {}

def should_save_weights_only(config):
    if config["architecture"] in keras_applications.keys():
        return True
    return False

###############################################################
## EXTRACT INFO FROM MODEL (SUMMARY AND LAYERS)
###############################################################

def get_model_input_shape(model, mf_path):

    input_shape = model.input_shape[1:3]

    # Check that model has an actual input shape
    if input_shape[0] == None or input_shape[1] == None:
        
        config = get_config(mf_path)
        architecture = config["architecture"]

        if not is_keras_application(architecture):
            raise IOError("You must provide an input shape for your architecture '{}'".format(architecture))

        return keras_applications[architecture].get("input_shape", (224, 224))

    else:
        return input_shape


def get_layers_as_list(model):
    layers = model.layers
    return [layer.__class__.__name__ for layer in layers]

def get_model_summary(model):
    summary_io = StringIO.StringIO()
    model.summary(print_fn=lambda line: summary_io.write(line + "\n"))
    return summary_io.getvalue()

def save_model_info(mf_path):
    model_info = {}

    # For SCORING
    model_info[constants.SCORING] = compute_model_info(mf_path, constants.SCORING)

    # For BEFORE_TRAIN
    model_info[constants.BEFORE_TRAIN] = compute_model_info(mf_path, constants.BEFORE_TRAIN)

    with open(get_file_path(mf_path, constants.MODEL_INFO_FILE), 'w') as f:
        json.dump(model_info, f)

def get_model_info(mf_path, goal):

    if os.path.isfile(get_file_path(mf_path, constants.MODEL_INFO_FILE)):
        model_info = json.loads(open(get_file_path(mf_path, constants.MODEL_INFO_FILE)).read())
        return model_info[goal]
    else:
        return compute_model_info(mf_path, goal)

def compute_model_info(mf_path, goal):
    model_info = {}

    if goal == constants.SCORING:
        
        model_and_pp_scoring = load_instantiate_keras_model_preprocessing(mf_path, goal=constants.SCORING, verbose=False)
        layers_scoring = get_layers_as_list(model_and_pp_scoring["model"])
        summary_scoring = get_model_summary(model_and_pp_scoring["model"])

        model_info = {
            "layers": layers_scoring,
            "summary": summary_scoring
        }

    elif goal == constants.BEFORE_TRAIN:

        model_and_pp_bt = load_instantiate_keras_model_preprocessing(mf_path, goal=constants.BEFORE_TRAIN, verbose=False)
        summary_bt = get_model_summary(model_and_pp_bt["model"])

        model_info = {
            "summary": summary_bt
        }

    return model_info

###################################################################################################################
## LOAD MODEL
###################################################################################################################

def load_instantiate_keras_model_preprocessing(mf_path, goal, input_shape=None, pooling=None, 
                                                reg=None, dropout=None, n_classes=None, verbose=True):
    config = get_config(mf_path)
    architecture = config["architecture"]
    trained_on = config["trained_on"]

    if is_keras_application(architecture):
        model_and_pp = load_keras_application(config, mf_path, goal, input_shape, pooling,reg, dropout, n_classes, verbose)

    # TODO : handle non keras application if such algorithms are available

    return model_and_pp

def load_keras_application(config, mf_path, goal, input_shape, pooling, reg, dropout, n_classes, verbose):
    architecture = config["architecture"]
    trained_on = config["trained_on"]
    retrained = config.get(constants.RETRAINED, False)
    top_params = config.get(constants.TOP_PARAMS, None)
    model_params = {}

    if trained_on != constants.IMAGENET:
        raise IOError("The architecture '{}', trained on '{}' cannot be found".format(architecture, trained_on))

    if retrained and top_params is None:
        raise IOError("Your config file is missing some parameters : '{}'".format(constants.TOP_PARAMS))

    if goal == constants.SCORING:

        if not retrained:
            
            if top_params is None:

                model = keras_applications[architecture]["model_func"](weights=None, include_top=True)
                model_weights_path = get_weights_path(mf_path, config)
                model.load_weights(model_weights_path)

            else:

                model = keras_applications[architecture]["model_func"](weights=None, include_top=False, input_shape=top_params["input_shape"])
                model, model_params = enrich_model(model, pooling, dropout, reg, n_classes, top_params, verbose)
                model_weights_path = get_weights_path(mf_path, config, constants.CUSTOM_TOP_SUFFIX)
                model.load_weights(model_weights_path)

        else:

            model = keras_applications[architecture]["model_func"](weights=None, include_top=False, input_shape=top_params["input_shape"])
            model, model_params = enrich_model(model, pooling, dropout, reg, n_classes, top_params, verbose)
            model_weights_path = get_weights_path(mf_path, config, constants.RETRAINED_SUFFIX)
            model.load_weights(model_weights_path)

    elif goal == constants.RETRAINING:

        if not retrained:

            model = keras_applications[architecture]["model_func"](weights=None, include_top=False, input_shape=input_shape)
            model_weights_path = get_weights_path(mf_path, config, constants.NOTOP_SUFFIX)
            model.load_weights(model_weights_path)
            model, model_params = enrich_model(model, pooling, dropout, reg, n_classes, top_params, verbose)
            model_params["input_shape"] = input_shape

        else:

            model = keras_applications[architecture]["model_func"](weights=None, include_top=False, input_shape=top_params["input_shape"])
            model, model_params = enrich_model(model, pooling, dropout, reg, n_classes, top_params, verbose)
            model_weights_path = get_weights_path(mf_path, config, constants.RETRAINED_SUFFIX)
            model.load_weights(model_weights_path)

    elif goal == constants.BEFORE_TRAIN:

        if not retrained:

            model = keras_applications[architecture]["model_func"](weights=None, include_top=False, input_shape=input_shape)
            model_weights_path = get_weights_path(mf_path, config, constants.NOTOP_SUFFIX)
            model.load_weights(model_weights_path)

        else:

            model = keras_applications[architecture]["model_func"](weights=None, include_top=False, input_shape=top_params["input_shape"])
            model, model_params = enrich_model(model, pooling, dropout, reg, n_classes, top_params, verbose)
            model_weights_path = get_weights_path(mf_path, config, constants.RETRAINED_SUFFIX)
            model.load_weights(model_weights_path)

    return {"model": model, "preprocessing": keras_applications[architecture]["preprocessing"], "model_params": model_params}

def select_param(param_name, param_val, top_params):
    return param_val if param_val is not None else top_params[param_name]

def enrich_model(base_model, pooling, dropout, reg, n_classes, params, verbose):

    # Init params if not done before
    params = {} if params is None else params
    
    # Loading appropriate params
    params["pooling"] = select_param("pooling", pooling, params)
    params["n_classes"] = select_param("n_classes", n_classes, params)

    x = base_model.layers[-1].output
        
    if params["pooling"] == 'None' :
        x = Flatten()(x)
    elif params["pooling"] == 'avg' :
        x = GlobalAveragePooling2D()(x)
    elif params["pooling"] == 'max' :
        x = GlobalMaxPooling2D()(x)

    if dropout is not None and dropout != 0.0 :
        x = Dropout(dropout)(x)
        if verbose:
            print("Adding dropout to model with rate: {}".format(dropout))

    regularizer = None
    if reg is not None:
        reg_l2 = reg["l2"]
        reg_l1 = reg["l1"]
        if (reg_l1 != 0.0) and (reg_l2 != 0.0) :
            regularizer = regularizers.l1_l2(l1=reg_l1, l2=reg_l2)
        if (reg_l1 == 0.0) and (reg_l2 != 0.0) :
            regularizer = regularizers.l2(reg_l2)
        if (reg_l1 != 0.0) and (reg_l2 == 0.0) :
            regularizer = regularizers.l1(reg_l1)
        if verbose:
            print("Using regularizer for model: {}".format(reg))
    
    predictions = Dense(params["n_classes"], activation='softmax', name='predictions', kernel_regularizer=regularizer)(x)
    model = Model(input=base_model.input, output=predictions)

    return model, params

###################################################################################################################
## GPU
###################################################################################################################

def load_gpu_options(should_use_gpu, list_gpu_str, gpu_allocation):
    gpu_options = {}
    if should_use_gpu:

        list_gpu = map(int, list_gpu_str.replace(" ", "").split(","))
        gpu_options["list_gpu"] = list_gpu
        gpu_options["n_gpu"] = len(list_gpu)

        config_tf = tf.ConfigProto()
        os.environ['CUDA_VISIBLE_DEVICES'] = list_gpu_str
        config_tf.gpu_options.per_process_gpu_memory_fraction = gpu_allocation
        session = tf.Session(config=config_tf)
        from keras.backend.tensorflow_backend import set_session
        set_session(session)
    else:
        deactivate_gpu()

    return gpu_options

def deactivate_gpu():
    os.environ["CUDA_VISIBLE_DEVICES"] = "-1"

def can_use_gpu():
    # Check that 'tensorflow-gpu' is installed on the current code-env
    import pip
    installed_packages = pip.get_installed_distributions()
    return "tensorflow-gpu" in [p.project_name for p in installed_packages]


###################################################################################################################
## FILES LOGIC
###################################################################################################################

def get_weights_path(mf_path, config, suffix="", should_exist=True):
    weights_filename =  get_weights_filename(mf_path, config, suffix)
    model_weights_path = get_file_path(mf_path, weights_filename)

    if not os.path.isfile(model_weights_path) and should_exist:
        raise IOError("No weigth file found")

    return model_weights_path

def get_weights_filename(mf_path, config, suffix=""):
    return "{}_{}_weights{}.h5".format(config["architecture"], config["trained_on"], suffix)

def get_config(mf_path):
    return json.loads(open(get_file_path(mf_path, constants.CONFIG_FILE)).read())

def write_config(mf_path, config):
    config_path = get_file_path(mf_path, constants.CONFIG_FILE)
    with open(config_path, 'w') as f:
        json.dump(config, f)

def check_managed_folder_filesystem(managed_folder):
    managed_folder_info = managed_folder.get_info()
    managed_folder_name = managed_folder_info["name"]
    connection_type = managed_folder_info["type"]

    if connection_type != "Filesystem" :
        raise IOError("The managed folder '{}' has a '{}' connection. Only Filesystem based managed folders are supported.".format(managed_folder_name, connection_type))

def get_file_path(folder_path, file_name):
    # Be careful to enforce that folder_path and file_name are actually strings
    return os.path.join(safe_str(folder_path), safe_str(file_name))

def safe_str(val):
    if sys.version_info > (3, 0):
        return str(val)
    else:
        if isinstance(val, unicode):
            return val.encode("utf-8")
        else:
            return str(val)

###################################################################################################################
## MISC.
###################################################################################################################

def get_predictions(model, batch, limit=5, min_threshold=0, labels_df=None):
    predictions = model.predict(batch)
    def id_pred(index):
        if labels_df is not None:
            return labels_df.loc[index].className
        else:
            return str(index)
    return [get_ordered_dict({id_pred(i): float(prediction[i]) for i in prediction.argsort()[-limit:] if float(prediction[i]) >= min_threshold}) for prediction in predictions]

def get_ordered_dict(predictions):
    return json.dumps(OrderedDict(sorted(predictions.items(), key=(lambda x: -x[1]))))


def preprocess_img(img_path, img_shape, preprocessing):
    img = load_img(img_path,target_size=img_shape)
    array = img_to_array(img)
    array = preprocessing(array)
    return array

def clean_custom_params(custom_params, params_type=""):

    def string_to_arg(string):
        if string.lower() == "true":
            res = True
        elif string.lower() == "false":
            res = False
        else :
            try :
                res = np.float(string)
            except ValueError:
                res = string
        return res

    cleaned_params = {}
    params_type = " '{}'".format(params_type) if params_type else ""
    for i, p in enumerate(custom_params) :
        if not p.get("name", False):
            raise IOError("The{} custom param #{} must have a 'name'".format(params_type, i))
        if not p.get("value", False):
            raise IOError("The{} custom param #{} must have a 'value'".format(params_type, i))
        name = p["name"]
        value = string_to_arg(p["value"])
        cleaned_params[name] = value
    return cleaned_params

###############################################################
## THREADSAFE GENERATOR / ITERATOR
## Inspired by :
##    https://github.com/fchollet/keras/issues/1638
##    http://anandology.com/blog/using-iterators-and-generators/
###############################################################

class ThreadsafeIterator(object):
    """Takes an iterator/generator and makes it thread-safe
    """

    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def next(self):
        with self.lock:
            return self.it.next()

def threadsafe_generator(f):
    """A decorator that takes a generator function and makes it thread-safe.
    """
    def g(*a, **kw):
        return ThreadsafeIterator(f(*a, **kw))
    return g

###############################################################
## MODEL CHECKPOINT FOR MULTI GPU
## When using multiple GPUs, we need to save the base model,
## not the one defined by multi_gpu_model
## see example: https://keras.io/utils/#multi_gpu_model
## Therefore, to save the model after each epoch by leveraging 
## ModelCheckpoint callback, we need to adapt it to save the 
## base model. To do so, we pass the base model to the callback.
## Inspired by: 
##   https://github.com/keras-team/keras/issues/8463#issuecomment-345914612
###############################################################

class MultiGPUModelCheckpoint(ModelCheckpoint):

    def __init__(self, filepath, base_model, monitor='val_loss', verbose=0,
                 save_best_only=False, save_weights_only=False,
                 mode='auto', period=1):
        super(MultiGPUModelCheckpoint, self).__init__(filepath, 
                                                      monitor=monitor,
                                                      verbose=verbose,
                                                      save_best_only=save_best_only,
                                                      save_weights_only=save_weights_only,
                                                      mode=mode,
                                                      period=period)
        self.base_model = base_model

    def on_epoch_end(self, epoch, logs=None):
        # Must behave like ModelCheckpoint on_epoch_end but save base_model instead

        # First retrieve model
        model = self.model

        # Then switching model to base model
        self.model = self.base_model

        # Calling super on_epoch_end
        super(MultiGPUModelCheckpoint, self).on_epoch_end(epoch, logs)

        # Resetting model afterwards
        self.model = model
