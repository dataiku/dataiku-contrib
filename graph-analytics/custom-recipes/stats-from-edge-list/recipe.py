# -*- coding: utf-8 -*-
import dataiku
from dataiku.customrecipe import *

import pandas as pd
import networkx as nx
from networkx.algorithms import bipartite


# Read recipe config
input_name = get_input_names_for_role('Input Dataset')[0]
output_name = get_output_names_for_role('Output Dataset')[0]

needs_eig = get_recipe_config()['eigenvector_centrality']
needs_clu = get_recipe_config()['clustering']
needs_tri = get_recipe_config()['triangles']
needs_clo = get_recipe_config()['closeness']
needs_pag = get_recipe_config()['pagerank']
needs_squ = get_recipe_config()['sq_clustering']

print get_recipe_config()

# Recipe input
df = dataiku.Dataset(input_name).get_dataframe()
print "[+] Dataset loaded..."

#Check input Dataset characteristics (only two columns)
if df.shape[1] != 2:
    raise ValueError('Your input Dataset must have only two columns')

# Creating the bipartite graph
graph = nx.Graph()
graph.add_edges_from(zip(df[df.columns[0]].values.tolist(),df[df.columns[1]].values.tolist()))
print "[+] Created bipartite graph..."

# Always run: nodes degree
print "[+] Computing degree..."
deg = pd.Series(nx.degree(graph), name='degree')

if needs_eig:
  print "[+] Computing eigenvector centrality..."
  eig = pd.Series(nx.eigenvector_centrality_numpy(graph), name='eigenvector_centrality')

if needs_clu:
  print "[+] Computing clustering coefficient..."
  clu = pd.Series(nx.clustering(graph), name='clustering_coefficient')

if needs_tri:
  print "[+] Computing number of triangles..."
  tri = pd.Series(nx.triangles(graph), name='triangles')

if needs_clo:
  print "[+] Computing closeness centrality..."
  clo = pd.Series(nx.closeness_centrality(graph), name='closeness_centrality')

if needs_pag:
  print "[+] Computing pagerank..."
  pag = pd.Series(nx.pagerank(graph), name='pagerank')

if needs_squ:
  print "[+] Computing square clustering..."
  squ = pd.Series(nx.square_clustering(graph), name='square_clustering_coefficient')

# Always run: connected components
_cco = {}
for i, c in enumerate(nx.connected_components(graph)):
    for e in c:
        _cco[e] = i
cco = pd.Series(_cco, name='connected_component_id')


# Putting all results together
stats = pd.DataFrame(deg)
stats = stats.join(cco)
if needs_eig:
  stats = stats.join(eig)
if needs_clu:
  stats = stats.join(clu)
if needs_tri:
  stats = stats.join(tri)
if needs_clo:
  stats = stats.join(clo)
if needs_pag:
  stats = stats.join(pag)
if needs_squ:
  stats = stats.join(squ)

stats = stats.reset_index().rename(columns={"index": "CREATE_GRAPH_OF_FROM_EDGES"})
_s = stats["connected_component_id"].value_counts().reset_index()
_s.columns = ['connected_component_id', 'connected_component_size']
stats = stats.merge(_s, on="connected_component_id", how="inner")
stats = stats.sort("CREATE_GRAPH_OF_FROM_EDGES")


# Recipe outputs
print "[+] Writing output dataset..."
graph = dataiku.Dataset(output_name)
graph.write_with_schema(stats)
