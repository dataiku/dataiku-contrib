import dataiku
from flask import json, request
import numpy as np
import pandas as pd

import dowhy
from dowhy.do_why import CausalModel


def new_query_yes_no(question, default=False):
    pass

# the original dowhy package prompts the user to acknowledge 
# with a y/n answer that the effect is not identifiable.
# Removing this.
dowhy.utils.cli_helpers.query_yes_no = new_query_yes_no


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


@app.route('/register-graph')
def register_graph():
    digraph = request.args.get('digraph')
    dataset = request.args.get('dataset')
    treatment_name = request.args.get('treatment')
    outcome_name = request.args.get('outcome')
    df = dataiku.Dataset(dataset).get_dataframe()

    model = CausalModel(
        data=df,
        treatment=treatment_name,
        outcome=outcome_name,
        graph=digraph,
    )

    identified_estimand = model.identify_effect()
    causal_estimate_reg = model.estimate_effect(identified_estimand,
                                                method_name="backdoor.linear_regression",
                                                test_significance=True)

    d = {'results': str(causal_estimate_reg)}

    return json.dumps(d)
