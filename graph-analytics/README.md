# Dataiku Graph Analytics Plugin

Current Version: 0.0.2

## Overview

This plugin offers two recipes to handle two kinds of graphs:

* Edges list (graph specified as a list of source_node / target_node couples)
* Bipartite graph (two kinds of items like product and transaction, transactions linking products together)

It can transform bipartite graphs into edge lists and extract node statistics from edge lists.

This plugin also provides a template webapp to visualize your graph and perform a community detection.

## Installation

This plugin must be installed following the [standard procedure](https://doc.dataiku.com/dss/latest/plugins/offline_install.html)

WARNING! If you want to use the webapp template provided, you have to create a [python code env](https://doc.dataiku.com/dss/latest/code-envs/operations-python.html) containing the following packages:

* networkx
* python-louvain
* flask
* matplotlib
* scipy

Once it's done, you can create your webapp with the provided template, activate the python backend, and chose the created code env.

## Usage

### Compute statistics from list of edges

This recipe takes the list of edges as input, and compute several graph statistics. Beware that some of the possible statistics can be quite long to compute depending of the size of the graph.

### Create a projected graph from a bipartite graph

This recipe transforms a bipartite graph (for exemple a list of transactions User - Product) into a list of edges for the projected graph (for exemple the graph of Products).
Be aware that his recipe can be memory expensive.

### Grap visualization webapp

This webapp gives you tha ability to visualize a graph and perform a community detection. You have to provide the input dataset, the columns corresponding to the source node and the target node, and th column corresponding to the intensity of the interaction (it can be constant to 1).
Then, the similarity parameter will give you the ability to take only edges with this minimum number of interactions.
The color of the resulting nodes correspond to the different communities.
