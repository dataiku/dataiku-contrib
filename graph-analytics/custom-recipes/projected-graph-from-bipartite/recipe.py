# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *

import pandas as pd
import networkx as nx
from networkx.algorithms import bipartite


# Read recipe config
input_name = get_input_names_for_role('Input Dataset')[0]
output_name = get_output_names_for_role('Output Dataset')[0]

CREATE_GRAPH_OF = get_recipe_config()['create_graph_of']
LINKED_BY = get_recipe_config()['linked_by']


# List of necessary columns
columns = []
columns.append(CREATE_GRAPH_OF)
columns.append(LINKED_BY)


# Recipe input
df = dataiku.Dataset(input_name).get_dataframe(columns=columns)
print "[+] Dataset loaded..."

# Delete nulls
df = df[(df[CREATE_GRAPH_OF].notnull()) & (df[LINKED_BY].notnull())]
print "[+] Removed null values..."

# Dedup
dd = df.groupby(columns).size().reset_index().rename(columns={0: 'w'})
print "[+] Created deduplicated dataset..."


# Creating the bipartite graph
G = nx.Graph()
G.add_nodes_from( dd[CREATE_GRAPH_OF].unique(),  bipartite=0 )
G.add_nodes_from( dd[LINKED_BY].unique(), bipartite=1 )
G.add_edges_from( zip(dd[CREATE_GRAPH_OF], dd[LINKED_BY]) )
print "[+] Created bipartite graph..."


# Projecting the main projected graph
graph = bipartite.projected_graph(G, dd[CREATE_GRAPH_OF].unique(), multigraph=False)
print "[+] Created projected graph..."


# Outputting the corresponding data frame
d = pd.DataFrame(list(graph.edges()))
d.columns = [CREATE_GRAPH_OF + '__1', CREATE_GRAPH_OF + '__2']


# Recipe outputs
print "[+] Writing output dataset..."
graph = dataiku.Dataset(output_name)
graph.write_with_schema(d)
