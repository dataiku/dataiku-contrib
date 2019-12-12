# import the classes for accessing DSS objects from the recipe
import dataiku

# Import the helpers for custom recipes
from dataiku.customrecipe import *

import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

import datetime

# Import stable_baselines_wrapper
from stable_baselines_training_wrapper import *

# Get the parameters
agent_var = get_recipe_config()['agent']

# Train the agent
train_agent(agent_var)
