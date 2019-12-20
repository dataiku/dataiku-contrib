# Reinforcement Learning in DSS

Create agents that learn to interact with environments using Reinforcement Learning.

This plugin provides tools to rapidly train, test and visualize reinforcement learning agents, on different video game environments.

This plugin relies on [Stable Baselines Library](https://stable-baselines.readthedocs.io/en/master/). Stable Baselines is an open-source library written in Python. We use it to run Reinforcement Learning agents and enable fast experimentation.

This plugin comes with 2 recipes and one webapp template:

- Train the agent (Recipe)
- Test the agent (Recipe)
- Visualize the agent testing results (Webapp template)

## Plugin Information
Version 0.1.0
Author Dataiku Labs (Thomas Simonini)
Released
Last updated
License : 
Source Code: [Github]()
Reporting issues: [Github](https://github.com/dataiku/dataiku-contrib/issues)

## How to Use
### Recipes
#### Train your agent
Use this recipe to train your selected agent. You can now select between Q-Learning and Deep Q-Learning agents. This recipe outputs the saved model of your trained agent and a JSON file containing the training info.

_Inputs_:
The folder containing the saved model and the JSON file.

_Outputs_:
Saved model (pickle file) and the training info JSON file.

#### Test your agent
Use this recipe to test your trained agent. This recipe will output a JSON file containing the testing info.

_Inputs_:
The folder containing the saved model.
The folder containing the replay video.

_Outputs_:
The testing info JSON file.

#### Visualize the results of your agent
Use this webapp template to visualize the training, testing info.
 

## Install In DSS
To install Plugin: Object detection in images in DSS, go to Administration > Plugins > Store and search for the plugin name.




