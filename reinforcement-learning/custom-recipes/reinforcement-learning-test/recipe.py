import dataiku

from dataiku.customrecipe import *

import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

import datetime

from stable_baselines_testing_wrapper import *

# Get the parameters
agent_var = get_recipe_config()['agent']

# Test the agent
test_agent(agent_var)







