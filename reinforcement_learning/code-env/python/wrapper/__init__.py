import numpy as np
import gym
import random

import dataiku

def train_agent(agent):
    
    # Get the parameters (the common ones)
    environment_var = get_recipe_config()['environment']
    agent_var = get_recipe_config()['agent']
    policy_var = get_recipe_config()['policy']
    gamma_var = get_recipe_config()['gamma']
    lr_var = get_recipe_config()['dqn_learning_rate']
    training_episodes_var = 5000

    
    # Create the JSON FILE and dump it into the output folder
    training_infos = {
        'name': environment_var,
        'agent': agent_var,
        'type': 'OpenAI Gym',
        'num_episodes': training_episodes_var,
        'lr': lr_var,
        'gamma': gamma_var,
        'policy': policy_var,
        'training_date': str(datetime.datetime.now())
    }
    
    saved_models = dataiku.Folder(get_output_names_for_role('main_output')[0])
    saved_models_info = saved_models.get_info()
    saved_models_path = saved_models.get_path()
    
    with open(saved_models_path + '/training_infos.json', 'w') as fp:
        json.dump(training_infos, fp)

    # Choose the agent
    if agent == "dqn":
        from stable_baselines.common.vec_env import DummyVecEnv
        from stable_baselines.deepq.policies import MlpPolicy
        from stable_baselines.deepq.policies import CnnPolicy
        from stable_baselines import DQN
        
        model = DQN(policy = policy_var, env = environment_var, gamma = gamma_var, learning_rate = lr_var)
        
    # Start the training and dump the model into the output folder
    print("========================== Start Training ==========================")
    model.learn(training_episodes_var)
    model_name = agent_var + "_" + environment_var
    print("Model Saved")
    model.save(saved_models_path + "/" + model_name)
    
    
    
    
    