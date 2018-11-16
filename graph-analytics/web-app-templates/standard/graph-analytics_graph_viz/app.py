import dataiku
from flask import request, json
import networkx as nx
from community import best_partition
from networkx.readwrite import json_graph

@app.route('/datasets')
def get_dataset_flow():
    client = dataiku.api_client()
    project_key = dataiku.default_project_key()
    project = client.get_project(project_key)
    datasets = project.list_datasets()
    dataset_names = [datasets[i]["name"] for i in range(len(datasets))]
    return json.jsonify({"dataset_names": dataset_names})

@app.route('/columns')
def get_columns():
    dataset = request.args.get("dataset")
    df = dataiku.Dataset(dataset).get_dataframe(limit=0)
    columns = list(df.columns.values)
    d = {'columns': columns}
    return json.dumps(d)

@app.route('/draw_graph')
def draw_graph():
    #get data
    project_key = dataiku.default_project_key()
    
    similarity = float(request.args.get('similarity'))
    node_source = request.args.get('node_source')
    node_target = request.args.get('node_target')
    interactions = request.args.get('interactions')
    dataset = request.args.get('dataset')
    name=project_key+'.'+dataset
    
    print name
   
    df=dataiku.Dataset(name).get_dataframe()
    
    df=df[df[interactions]>similarity]
    df=df[[node_source,node_target,interactions]]
    df.columns=['source','target','weight']
    
    print "%d rows" % df.shape[0]
    G=nx.Graph()
    G.add_edges_from(zip(df.source,df.target))

    print nx.info(G)

    # degree
    for node, val in dict(nx.degree(G)).iteritems(): 
        G.node[node]['degree'] = val
    # pagerank
    for node, val in dict(nx.pagerank(G)).iteritems(): 
        G.node[node]['pagerank'] = val
    # connected components
    components =  sorted(nx.connected_components(G), key = len, reverse=True)
    for component,nodes in enumerate(components):
        for node in nodes:
            G.node[node]['cc'] = component
    # community
    partition = best_partition(G)
    for node, cluster in dict(partition).iteritems():
        G.node[node]['community'] = cluster
    
    # convert to JSON
    data = json_graph.node_link_data(G)
    
    #fix for networkx>=2.0 change of API
    if nx.__version__ > 2:
        dict_name_id = {data["nodes"][i]["id"] : i for i in xrange(len(data["nodes"]))}
        for link in data["links"]:
            link["source"] = dict_name_id[link["source"]]
            link["target"] = dict_name_id[link["target"]]
        
    return json.dumps({"status" : "ok", "graph": data})

