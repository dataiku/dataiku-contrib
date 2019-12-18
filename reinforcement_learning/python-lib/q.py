# Step 0: Import the dependencies
import numpy as np
import gym
import random

def train_q(environment_var,
                     agent_var,
                      gamma_var,
                      lr_var,
                      total_episodes_var,
                      q_max_steps_var,
                      q_epsilon_var,
                      q_max_epsilon_var,
                      q_min_epsilon_var,
                      q_decay_rate_var):
            
    # Step 1: Create the environment
    env = gym.make(environment_var)

    # Step 2: Create the Q-table and initialize it
    action_size = env.action_space.n
    state_size = env.observation_space.n
    qtable = np.zeros((state_size, action_size))

    # Step 3: Create the hyperparameters
    total_episodes = total_episodes_var       
    learning_rate = lr_var           
    max_steps = q_max_steps_var                # Max steps per episode
    gamma = gamma_var                  # Discounting rate

    # Exploration parameters
    epsilon = q_epsilon_var               # Exploration rate
    max_epsilon = q_max_epsilon_var             # Exploration probability at start
    min_epsilon = q_min_epsilon_var           # Minimum exploration probability 
    decay_rate = q_decay_rate_var            # Exponential decay rate for exploration prob

    # Step 4: The Q learning algorithm 
    # List of rewards
    rewards = []

    # 2 For life or until learning is stopped
    for episode in range(total_episodes):
        # Reset the environment
        state = env.reset()
        step = 0
        done = False
        total_rewards = 0
        step = 0
    
        for step in range(max_steps):
            # 3. Choose an action a in the current world state (s)
            ## First we randomize a number
            exp_exp_tradeoff = random.uniform(0, 1)
        
            ## If this number > greater than epsilon --> exploitation (taking the biggest Q value for this state)
            if exp_exp_tradeoff > epsilon:
                action = np.argmax(qtable[state,:])

            # Else doing a random choice --> exploration        
            else:
                action = env.action_space.sample()

            # Take the action (a) and observe the outcome state(s') and reward (r)
            new_state, reward, done, info = env.step(action)

            # Update Q(s,a):= Q(s,a) + lr [R(s,a) + gamma * max Q(s',a') - Q(s,a)]
            qtable[state, action] = qtable[state, action] + learning_rate * (reward + gamma * np.max(qtable[new_state, :]) - qtable[state, action])
    
            total_rewards += reward
        
            # Our new state is state
            state = new_state
        
            # If done (if we're dead) : finish episode
            if done == True: 
                break
        
        # Reduce epsilon (because we need less and less exploration)
        epsilon = min_epsilon + (max_epsilon - min_epsilon)*np.exp(-decay_rate*episode) 
        rewards.append(total_rewards)

    # Return the qtable
    return qtable


            