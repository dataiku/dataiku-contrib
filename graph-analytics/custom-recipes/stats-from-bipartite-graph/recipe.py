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

needs_eig = get_recipe_config()['eigenvector_centrality']
needs_clu = get_recipe_config()['clustering']
needs_tri = get_recipe_config()['triangles']
needs_clo = get_recipe_config()['closeness']
needs_pag = get_recipe_config()['pagerank']
needs_squ = get_recipe_config()['sq_clustering']

print get_recipe_config()

# List of necessary columns
columns = []
columns.append(CREATE_GRAPH_OF)
columns.append(LINKED_BY)


# Recipe input
print "[+] Loading dataset..."
df = dataiku.Dataset(input_name).get_dataframe(columns=columns)

# Delete nulls
print "[+] Dataset loaded, removing nulls..."
df = df[(df[CREATE_GRAPH_OF].notnull()) & (df[LINKED_BY].notnull())]

# Dedup
print "[+] Removed null values, deduping..."
dd = df.groupby(columns).size().reset_index().rename(columns={0: 'w'})


# Creating the bipartite graph
print "[+] Created deduplicated dataset, creating bipartite graph..."
G = nx.Graph()
G.add_nodes_from( dd[CREATE_GRAPH_OF].unique(),  bipartite=0 )
G.add_nodes_from( dd[LINKED_BY].unique(), bipartite=1 )
G.add_edges_from( zip(dd[CREATE_GRAPH_OF], dd[LINKED_BY]), weight=dd['w'] )

# Projecting the main projected graph
print "[+] Created bipartite graph, creating projected graph"
graph = bipartite.projected_graph(G, dd[CREATE_GRAPH_OF].unique(), multigraph=False)
print "[+] Created projected graph..."


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
print "[+] Computing connected components"
_cco = {}
for i, c in enumerate(nx.connected_components(graph)):
    for e in c:
        _cco[e] = i
cco = pd.Series(_cco, name='connected_component_id')


# Putting all results together
print "[+] Preparing output"
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

stats = stats.reset_index().rename(columns={"index": CREATE_GRAPH_OF})
_s = stats["connected_component_id"].value_counts().reset_index()
_s.columns = ['connected_component_id', 'connected_component_size']
stats = stats.merge(_s, on="connected_component_id", how="inner")
stats = stats.sort(CREATE_GRAPH_OF)


# Recipe outputs
print "[+] Writing output dataset..."
graph = dataiku.Dataset(output_name)
graph.write_with_schema(stats)
