import numpy as np
import gym
import random

import dataiku

import datetime

# Import the helpers for custom recipes
from dataiku.customrecipe import *

def train_agent(agent):
    # Get the parameters (the common ones)
    environment_var = get_recipe_config()['environment']
    environment_library_var = get_recipe_config()['environment_library']
    
    agent_var = get_recipe_config()['agent']
    policy_var = get_recipe_config()['policy']
    
    gamma_var = get_recipe_config()['gamma']
    lr_var = get_recipe_config()['learning_rate']
    
    total_timesteps_var = int(get_recipe_config()['total_timesteps'])
        
    # Create the JSON FILE and dump it into the output folder
    training_infos = {
        'environment': environment_var + " " + environment_library_var,
        'agent': agent_var,
        'policy': policy_var,
        'gamma': gamma_var,
        'lr': lr_var,
        'trainingdate': str(datetime.datetime.now()),
        'total_timesteps': total_timesteps_var,
    }
    
    saved_models = dataiku.Folder(get_output_names_for_role('main_output')[0])
    saved_models_info = saved_models.get_info()
    saved_models_path = saved_models.get_path()
    
    with open(saved_models_path + '/training_infos.json', 'w') as fp:
        json.dump(training_infos, fp)

    # Choose the agent
    if agent == "dqn":
         # First check if other hyperparameters (other than common hyperparameters)
        dqn_exploration_fraction_var = get_recipe_config()["dqn_exploration_fraction"]
        dqn_exploration_final_eps_var = get_recipe_config()['dqn_exploration_final_eps']
        
        dqn_buffer_size_var = int(get_recipe_config()['dqn_buffer_size'])
        dqn_prioritized_replay_var = get_recipe_config()["dqn_prioritized_replay"]
        
        dqn_double_q_var = get_recipe_config()['dqn_double_q']
        dqn_target_network_update_freq_var = int(get_recipe_config()["dqn_target_network_update_freq"])
        
        dqn_train_freq_var = int(get_recipe_config()["dqn_train_freq"])
        dqn_batch_size_var = int(get_recipe_config()['dqn_batch_size'])
        
        
        
        # Advanced hyperparameters (stand by for the first version)
        #advanced_params = get_recipe_config()["advanced_params"]
        
        # Import dqn
        from dqn import train_dqn
        model = train_dqn(total_timesteps_var,
                          policy_var, 
                          environment_var, 
                          gamma_var, 
                          lr_var, 
                          dqn_buffer_size_var,
                          dqn_exploration_fraction_var,
                          dqn_exploration_final_eps_var,
                          dqn_train_freq_var,
                          dqn_batch_size_var,
                          dqn_double_q_var,
                          dqn_target_network_update_freq_var,
                          dqn_prioritized_replay_var)
    
    model_name = agent_var + "_" + environment_var
    model.save(saved_models_path + "/" + model_name)
    print("Model Saved")
    
    
    
    
    