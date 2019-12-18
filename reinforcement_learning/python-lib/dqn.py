from stable_baselines.common.vec_env import DummyVecEnv
from stable_baselines.deepq.policies import MlpPolicy
from stable_baselines.deepq.policies import CnnPolicy
from stable_baselines import DQN
  
def train_dqn(total_timesteps_var,
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
              dqn_prioritized_replay_var):
    

    model = DQN(policy = policy_var, 
                          env = environment_var, 
                          gamma = gamma_var, 
                          learning_rate = lr_var, 
                          buffer_size = dqn_buffer_size_var,
                          exploration_fraction = dqn_exploration_fraction_var,
                          exploration_final_eps = dqn_exploration_final_eps_var,
                          train_freq = dqn_train_freq_var,
                          batch_size = dqn_batch_size_var,
                          double_q = dqn_double_q_var,
                          target_network_update_freq = dqn_target_network_update_freq_var,
                          prioritized_replay = dqn_prioritized_replay_var)
    
    # Start the training and dump the model into the output folder
    print("========================== Start Training ==========================")
    model.learn(total_timesteps_var)
    return model

def test_dqn(saved_models_path, agent_var, environment_var):
    model = DQN.load(saved_models_path + "/" + agent_var + "_" + environment_var)
    return model
