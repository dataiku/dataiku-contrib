import dataiku
def do(payload, config, plugin_config, inputs):
    """
    Create list of options of projects in instance.
    """
    projects = dataiku.api_client().list_projects()
    choices = []
    for project in projects:
        choices.append({"value": project.get('projectKey'), "label": project.get('projectKey')})
    return {"choices": choices}