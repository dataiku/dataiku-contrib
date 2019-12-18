import numpy as np
import gym
import random

import dataiku

import datetime
import time 

import pickle

# Import the helpers for custom recipes
from dataiku.customrecipe import *

from q import train_q

def train_q_agent(agent):
    # Get the parameters (the common ones)
    environment_var = get_recipe_config()['environment']
    environment_library_var = get_recipe_config()['environment_library']
    
    agent_var = get_recipe_config()['agent']
    
    gamma_var = get_recipe_config()['gamma']
    lr_var = get_recipe_config()['learning_rate']
    
    total_episodes_var = int(get_recipe_config()['total_episodes'])
    
    # Create the JSON FILE and dump it into the output folder
    training_infos = {
        'environment': environment_var + " " + environment_library_var,
        'environmentName': environment_var,
        'agent': agent_var,
        'gamma': gamma_var,
        'policy': "Q Learning",
        'lr': lr_var,
        'trainingdate': str(datetime.datetime.now()),
        'total_timesteps': str(total_episodes_var) + " " + "episodes"
        
    }
    
    saved_models = dataiku.Folder(get_output_names_for_role('main_output')[0])
    saved_models_info = saved_models.get_info()
    saved_models_path = saved_models.get_path()
    
    with open(saved_models_path + '/training_infos.json', 'w') as fp:
        json.dump(training_infos, fp)

       
    # First check if other hyperparameters (other than common hyperparameters)
    q_max_steps_var = int(get_recipe_config()['q_max_steps'])
    q_epsilon_var = get_recipe_config()['q_epsilon']
    q_max_epsilon_var = get_recipe_config()['q_max_epsilon']
    q_min_epsilon_var = get_recipe_config()['q_min_epsilon']
    q_decay_rate_var = get_recipe_config()['q_decay_rate']
    
    # Train the Qtable
    q_model = train_q(environment_var,
                     agent_var,
                      gamma_var,
                      lr_var,
                      total_episodes_var,
                      q_max_steps_var,
                      q_epsilon_var,
                      q_max_epsilon_var,
                      q_min_epsilon_var,
                      q_decay_rate_var)
                      
    print("Q MODEL", q_model)
                      
    
    # Save the model 
    #model_name = agent_var + "_" + environment_var + "_" +  str(time.time())
    model_name = agent_var + "_" + environment_var
    
    with open(saved_models_path + '/' + model_name + '.pickle', 'wb') as f:
        pickle.dump(q_model, f)

    print("Model Saved")
    
       
    
    
    