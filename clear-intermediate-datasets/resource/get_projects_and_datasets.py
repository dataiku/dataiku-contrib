import dataiku
def do(payload, config, plugin_config, inputs):
    """
    Create list of options of projects in instance.
    """
    client = dataiku.api_client()
    # Set project dropdown
    if payload.get('parameterName') == "project_key":
        projects = client.list_projects()
        choices = []
        for project in projects:
            choices.append({"value": project.get('projectKey'), "label": project.get('projectKey')})
        return {"choices": choices}
    # Set dataset multiselect box - Display list of datasets of current project if no project has been selected
    elif payload.get('parameterName') == "datasets_to_exclude" and config.get("project_key", None) == None:
        project = client.get_default_project()
        datasets = project.list_datasets()
        choices = []
        for dataset in datasets:
            choices.append({"value": dataset.get('name'), "label": dataset.get('name')})
        return {"choices": choices}
    # Set dataset multiselect box - Display list of datasets if a project has been selected
    elif payload.get('parameterName') == "datasets_to_exclude":
        project = client.get_project(config.get("project_key"))
        datasets = project.list_datasets()
        choices = []
        for dataset in datasets:
            choices.append({"value": dataset.get('name'), "label": dataset.get('name')})
        return {"choices": choices}    
