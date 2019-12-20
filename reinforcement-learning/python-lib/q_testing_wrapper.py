import numpy as np
import gym
from gym import wrappers
import random

import datetime
import time

import dataiku
from dataiku.customrecipe import *
import pickle

def test_q_agent(agent):
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
    
    # Test the agent
    with open(saved_models_path + '/training_infos.json') as f: 
        training_infos = json.load(f)

    # Load the model
    qtable = pickle.load(open(saved_models_path + "/" + agent_var + "_" + environment_var + ".pickle", 'rb'))

    env = gym.make(environment_var)
    # Make the video
    # env = wrappers.Monitor(env, saved_replays_path + "/", video_callable=lambda episode_id: episode_id%10==0, force=True)
    
    scores = []
    
    env.reset()
    rewards = 0
    for episode in range(100):
        state = env.reset()
        step = 0
        done = False
        
        print("****************************************************")
        print("EPISODE ", episode)
        for step in range(99):
        
            # Take the action (index) that have the maximum expected future reward given that state
            action = np.argmax(qtable[state,:])
        
            new_state, reward, done, info = env.step(action)
            rewards += reward
        
            if done:
                # Here, we decide to only print the last state (to see if our agent is on the goal or fall into an hole)
                #env.render()
            
                # We print the number of step it took.
                break
            state = new_state
    env.close()
    average_score = int((rewards/100)*100)
    percentage_score = (rewards/100)*100

    testing_infos = {
        'agent_name': "Q-Learning Agent",
        'score': scores,
        'average_score': average_score,
        'percentage_score': percentage_score 
    }

    infos = dict(training_infos, **testing_infos)

    output_json_name = training_infos["environment"] +"_"+ training_infos["agent"] + "_" + str(time.time()) + ".json"

    with open(saved_replays_path + "/" + output_json_name, 'w') as fp:
        json.dump(infos, fp)
