import numpy as np
import gym
from gym import wrappers
import random

import datetime
import time

import dataiku
from dataiku.customrecipe import *

def test_agent(agent):
    # Get the parameters (the common ones)
    environment_var = get_recipe_config()['environment']
    environment_library_var = get_recipe_config()['environment_library']
    agent_var = get_recipe_config()['agent']
    
    # Fetch the saved_model and saved_replays folder
    saved_models = dataiku.Folder(get_input_names_for_role('main_input')[0])
    saved_models_info = saved_models.get_info()
    saved_models_path = saved_models.get_path()

    saved_replays = dataiku.Folder(get_output_names_for_role('main_output')[0])
    saved_replays_info = saved_replays.get_info()
    saved_replays_path = saved_replays.get_path()
    
    # Choose the agent
    if agent == "dqn":
        from dqn import test_dqn
        model = test_dqn(saved_models_path, agent_var, environment_var)
        
    if agent != "q":
    
        # Test the agent
        with open(saved_models_path + '/training_infos.json') as f: 
            training_infos = json.load(f)

        env = gym.make(environment_var)
    
        scores = []

        for episode in range(10):
            obs = env.reset()
            step = 0
            done = False
            total_rewards = 0
            print("****************************************************")
            print("EPISODE ", episode)
        
            while True:
                action, _states = model.predict(obs)
                obs, rewards, dones, info = env.step(action)

                total_rewards += rewards

                if dones:
                    scores.append(total_rewards)
                    print ("Score", total_rewards)
                    break
        
            env.close()
            average_score = sum(scores)/10


        testing_infos = {
            'agent_name': "Deep Q-Learning Agent",
            'score': scores,
            'average_score': average_score 
        }

        infos = dict(training_infos, **testing_infos)

        output_json_name = training_infos["environment"] +"_"+ training_infos["agent"] + "_" + str(time.time()) + ".json"

        with open(saved_replays_path + "/" + output_json_name, 'w') as fp:
            json.dump(infos, fp)
            
    elif agent == 'q':
        from q_testing_wrapper import test_q_agent
        test_q_agent(agent)
            