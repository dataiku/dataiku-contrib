import time
import os.path
import datetime
import dataiku
import dataikuapi
from dataiku.runnables import Runnable
from dataikuapi.utils import DataikuException


class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        """
        :param project_key: the project in which the runnable executes
        :param config: the dict of the configuration of the object
        :param plugin_config: contains the plugin settings
        """
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        
        # verify inputs
        bundle_id = self.config.get("bundle_id", "")
        if bundle_id == "":
            raise Exception("bundle_id is required")

        remote_host = self.config.get("remote_host", "")
        if remote_host == "":
            raise Exception("destination is required")

        api_key = self.config.get("api_key", "")
        if api_key == "":
            raise Exception("API key is required")

        activate_scenarios = self.config.get("activate_scenarios")
        
        # get client and connect to project
        client = dataiku.api_client()
        project = client.get_project(self.project_key)
        
        # use public python api to get access to remote host
        remote_client = dataikuapi.DSSClient(remote_host, api_key)
        html = '<div> Successfully connected to remote host: %s</div>' %(remote_client.host)
        
        # get list of connections used in the initial project
        datasets = project.list_datasets()
        connections_used = []

        for dataset in datasets:
            try:
                connections_used.append(dataset['params']['connection'])
            except:
                continue
        
        connections_used = list(set(connections_used))
        
        # get list of connections in remote project
        remote_connections_list = remote_client.list_connections()
        remote_connections_names = remote_connections_list.keys()
        
        # check if connections used in the initial project also exist on the remote instance
        for connection in connections_used:
            if connection not in remote_connections_names:
                error_msg = 'Failed - Connection %s used in initial project does not exist on instance %s.' %(connection, remote_host)
                raise Exception(error_msg)
                
            else:
                continue
        
        html += '<div><div>All connections exist on both instances</div>'
        
        # check if any plugins installed in the original instance are not installed in remote instance
        # get list of plugins in original instance
        original_plugin_names = {}
        for plugin in client.list_plugins():
            original_plugin_names[plugin['meta']['label']] = plugin['version']
        
        # compare to list of plugins in remote instance
        remote_plugin_names = {}
        for plugin in remote_client.list_plugins():
            remote_plugin_names[plugin['meta']['label']] = plugin['version']
        
        missing_plugins = {k: original_plugin_names[k] for k in original_plugin_names if k not in remote_plugin_names or original_plugin_names[k] == remote_plugin_names[k]}
        
        if len(missing_plugins) > 0:
            html += '<div> <b> Warning: the following plugins (and versions) are installed on this instance, but not on the remote instance. Please ensure that your project does not use these plugins before proceeding. </b> </div>'
            html += '<table>'
            html += '<tr>'
            html += '<th>Plugin</th>'
            html += '<th>Version</th>'
            html += '</tr>'
            
        for plugin in missing_plugins:
            html += '<tr>'
            html += '<td> %s </td>' % (plugin)
            html += '<td> %s </td>' % (missing_plugins[plugin])
            html += '</tr>'
        
        if len(missing_plugins) > 0:
            html += '</table>'
        
        ''' TRIGGER THE BELOW SCENARIO CODE IF CHECKBOX CHECKED '''
        # get a list of active scenario ids on the project
        if activate_scenarios:
            project_active_scenarios = []
        
            for scenario in project.list_scenarios():
                s = project.get_scenario(scenario['id'])
                scenario_def = s.get_definition()
                if scenario_def['active']:
                    project_active_scenarios.append(scenario_def['id'])
        
        # create the bundle (verify that the bundle_id does not exist)
        try:
            project.export_bundle(bundle_id)
            html += '<div><div>Successfully created bundle: %s</div>'  % (bundle_id)
        except DataikuException as de:
            error_msg = 'Failed - Bundle %s already exists for project %s' % (bundle_id, self.project_key)
            raise Exception(error_msg)
        
        # check if there are any projects in remote instance
        try:
            remote_projects = remote_client.list_project_keys()
        except:
            remote_projects = []
        
        if not self.project_key in remote_projects:
            remote_client.create_project(self.project_key, self.project_key, 'admin', 'placeholder description')
            html += '<div> Project %s does not exist on instance %s.  Creating it.</div>' %(self.project_key, remote_host)
        else:
            html += '<div> Project %s already exists on instance %s.  Updating with new bundle version %s.</div>' %(self.project_key, remote_host, bundle_id)
        
        # connect to remote project
        remote_project = remote_client.get_project(self.project_key)
        
        with project.get_exported_bundle_archive_stream(bundle_id) as fp:
            remote_project.import_bundle_from_stream(fp.content)
        
        # "preload bundle" to create/update custom code environments used throughout the project
        preload = remote_project.preload_bundle(bundle_id)
        
        # activate bundle
        remote_project.activate_bundle(bundle_id)
        html += '<div> Bundle activated </div>'

        ''' TRIGGER THE BELOW SCENARIO CODE IF CHECKBOX CHECKED '''
        # activate scenarios that were active on design instance
        if activate_scenarios:
            for active_scenario_id in project_active_scenarios:
                scenario = remote_project.get_scenario(active_scenario_id)
                scenario_def = scenario.get_definition()
                scenario_def['active'] = True
                scenario.set_definition(scenario_def)
        
        html += '</div>'
        return html
    
    
    
    
