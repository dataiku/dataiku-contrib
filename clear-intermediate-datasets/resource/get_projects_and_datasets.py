import dataiku
from utils import append_dropdown_choices

def do(payload, config, plugin_config, inputs):
    """
    Populate choices for projects and datasets in the initial macro interface
    """
    client = dataiku.api_client()
    
    # Set project dropdown
    parameter_name = payload.get('parameterName')
    if parameter_name == "project_key":
        projects = client.list_projects()
        return {"choices": append_dropdown_choices(projects, 'projectKey')}
    
    # Set dataset multiselect box
    elif parameter_name == "datasets_to_exclude":
        if config.get("project_key", None) == None:
            project = client.get_default_project()
        else:
            project = client.get_project(config.get("project_key"))
        datasets = project.list_datasets()
        return {"choices": append_dropdown_choices(datasets, 'name')}
