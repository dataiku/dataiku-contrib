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

node_A=get_recipe_config()['node_A']
node_B=get_recipe_config()['node_B']

print get_recipe_config()

# Recipe input
df = dataiku.Dataset(input_name).get_dataframe()
print "[+] Dataset loaded..."

# Creating the bipartite graph
graph = nx.Graph()
graph.add_edges_from(zip(df[node_A].values.tolist(),df[node_B].values.tolist()))
print "[+] Created bipartite graph..."

# Always run: nodes degree
print "[+] Computing degree..."
deg = pd.Series(nx.degree(graph), name='degree')
stats = pd.DataFrame(list(deg),columns=['node_name','degree'])

if needs_eig:
    print "[+] Computing eigenvector centrality..."
    eig = pd.Series(nx.eigenvector_centrality_numpy(graph), name='eigenvector_centrality').reset_index()
    eig.columns=['node_name','eigenvector_centrality']

if needs_clu:
    print "[+] Computing clustering coefficient..."
    clu = pd.Series(nx.clustering(graph), name='clustering_coefficient').reset_index()
    clu.columns=['node_name','clustering_coefficient']

if needs_tri:
    print "[+] Computing number of triangles..."
    tri = pd.Series(nx.triangles(graph), name='triangles').reset_index()
    tri.columns=['node_name','triangles']

if needs_clo:
    print "[+] Computing closeness centrality..."
    clo = pd.Series(nx.closeness_centrality(graph), name='closeness_centrality').reset_index()
    clo.columns=['node_name','closeness_centrality']

if needs_pag:
    print "[+] Computing pagerank..."
    pag = pd.Series(nx.pagerank(graph), name='pagerank').reset_index()
    pag.columns=['node_name','pagerank']

if needs_squ:
    print "[+] Computing square clustering..."
    squ = pd.Series(nx.square_clustering(graph), name='square_clustering_coefficient').reset_index()
    squ.columns=['node_name','square_clustering_coefficient']

# Always run: connected components
_cco = {}
for i, c in enumerate(nx.connected_components(graph)):
    for e in c:
        _cco[e] = i
cco = pd.Series(_cco, name='connected_component_id').reset_index()
cco.columns=['node_name','connected_component_id']

# Putting all together


stats = stats.merge(cco,how='left')
if needs_eig:
    stats = stats.merge(eig,how='left')
if needs_clu:
    stats = stats.merge(clu,how='left')
if needs_tri:
    stats = stats.merge(tri,how='left')
if needs_clo:
    stats = stats.merge(clo,how='left')
if needs_pag:
    stats = stats.merge(pag,how='left')
if needs_squ:
    stats = stats.merge(squ,how='left')


_s = stats["connected_component_id"].value_counts().reset_index()
_s.columns = ['connected_component_id', 'connected_component_size']
stats = stats.merge(_s, on="connected_component_id", how="left")

# Recipe outputs
print "[+] Writing output dataset..."
graph = dataiku.Dataset(output_name)
graph.write_with_schema(stats)
