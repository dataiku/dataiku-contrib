import time
import os.path
import datetime
import dataiku
import dataikuapi
from dataiku.runnables import Runnable
from dataikuapi.utils import DataikuException
import os
import re

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
    
    def _get_next_git_remote_name_url(self, proj_git_config):
        """
        :param proj_git_config: the git config serialized text file for a project
        """
        git_remote_name = ''
        url = ''
        
        # parse each line in the git config and return the next git remote name and corresponding url
        for line in proj_git_config:
            if line.startswith("[remote"):
                git_remote_name = re.findall(r'"([^"]*)"', line)[0]
                break

        for line in proj_git_config:
            try:
                url = re.findall(r'url\s=\s(.+)',line)[0]
                break
            except:
                continue
        return git_remote_name, url
    
    def _get_git_remote_lines(self, name, url):
        """
        :param name: the git remote name
        :param url: the git remote url
        """
        
        # create lines of text to add to the new git config file
        line_1 = '[remote "%s"]' % (name)
        line_2 = "\turl = %s" % (url)
        line_3 = "\tfetch = +refs/head/*:refs/remotes/%s/*" % (name)
        
        return {'line_1': line_1,
                'line_2': line_2,
                'line_3': line_3}
    
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
        
        ignore_ssl_certs = self.config.get("ignore_ssl_certs")
        if ignore_ssl_certs:
            remote_client._session.verify = False
        
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
        
        # assign git remote on initial project if desired
        assign_git_remote = self.config.get("assign_git_remote")
        
        os.environ['DKU_CURRENT_PROJECT_KEY'] = self.project_key
   
        if assign_git_remote:

            new_git_remote_name = self.config.get("git_remote_name")
            new_git_remote_url = self.config.get("git_remote_url")
            
            # get the path of remote git config files for this project
            dss_data_dir = dataiku.get_custom_variables()["dip.home"]
            path_to_git_remote_config = dss_data_dir + '/config/projects/' + self.project_key + '/.git/config'
            
            # count the number of existing git remotes for the initial project
            with open(path_to_git_remote_config,"r") as project_git_config:
                previous_remotes = 0
                for line in project_git_config:
                    if line.startswith("[remote"):
                        previous_remotes += 1
            
            # store the name/url for each git remote for the initial project
            with open(path_to_git_remote_config,"r") as project_git_config:
                remotes_list = []
                while previous_remotes > 0:
                    remote_name, remote_url = self._get_next_git_remote_name_url(project_git_config)
                    remotes_list.append({"name":remote_name,
                                         "url":remote_url})
                    previous_remotes -= 1
            
            git_remotes_config_lines = {}
            existing_git_remote_counter = 0
            
            # if existing remotes, add their corresponding git config file text lines to a dictionary -- to store for later
            if len(remotes_list) > 0:
                git_remotes_config_lines['existing'] = []
                for existing_git_remote in remotes_list:
                    existing_git_remote_counter += 1
                    single_git_remote_lines = self._get_git_remote_lines(existing_git_remote['name'], existing_git_remote['url'])
                    git_remotes_config_lines['existing'].append(single_git_remote_lines)
                
            git_remotes_config_lines['new'] = self._get_git_remote_lines(new_git_remote_name, new_git_remote_url)
            
            # if existing remotes, then delete them from the initial project git config file
            if len(remotes_list) > 0:
                # read lines from initial file
                with open(path_to_git_remote_config, "r") as project_git_config:
                    git_remote_config_lines = project_git_config.readlines()

                # delete all existing git remote lines
                with open(path_to_git_remote_config, "w") as project_git_config:

                    # iterate through each line in config
                    for line in git_remote_config_lines:

                        # iterate through each existing git remote
                        for existing_git_remote in git_remotes_config_lines['existing']:

                            # check if line not part of existing git remote -- only write back non-existing-remote lines
                            if line.strip("\n") != existing_git_remote['line_1'] and line.strip("\n") != existing_git_remote['line_2'] and line.strip("\n") != existing_git_remote['line_3']:
                                continue
                            else:
                                break
                        else:
                            project_git_config.write(line)
            
            
            # open the (now empty) config file and append the new git remote lines
            with open(path_to_git_remote_config, "a") as project_git_config:
                project_git_config.write('{}\n{}\n{}\n'.format(git_remotes_config_lines['new']['line_1'], git_remotes_config_lines['new']['line_2'], git_remotes_config_lines['new']['line_3']))
            
            html += '<div> Git Remote Assigned on Initial Design Instance </div>'
        
        # create the bundle (verify that the bundle_id does not exist)
        try:
            project.export_bundle(bundle_id)
            html += '<div><div>Successfully created bundle: %s</div>'  % (bundle_id)
        except DataikuException as de:
            error_msg = 'Failed - Bundle %s already exists for project %s' % (bundle_id, self.project_key)
            
            # FIX GIT REMOTE STUFF IF BUNDLE ACTIVATION FAILS
            #if assigned a git remote to initial project, go back and delete it, and add back in initial git remotes
            
            if assign_git_remote:

                # read lines from initial file
                with open(path_to_git_remote_config, "r") as project_git_config:
                    git_remote_config_lines = project_git_config.readlines()

                # write back only the lines from before adding the remote
                with open(path_to_git_remote_config, "w") as project_git_config:
                    for line in git_remote_config_lines:
                        if line.strip("\n") != git_remotes_config_lines['new']['line_1'] and line.strip("\n") != git_remotes_config_lines['new']['line_2'] and line.strip("\n") != git_remotes_config_lines['new']['line_3']:
                            project_git_config.write(line)

                html += '<div> Git Remote Deleted from Initial Design Instance </div>'

                # open the config file and append the previous git remote lines
                if len(remotes_list) > 0:
                    with open(path_to_git_remote_config, "a") as project_git_config:
                        for existing_git_remote in git_remotes_config_lines['existing']:
                            project_git_config.write('{}\n{}\n{}\n'.format(existing_git_remote['line_1'], existing_git_remote['line_2'], existing_git_remote['line_3']))
                    html += '<div> Initial Git Remotes added back to Initial Design Instance </div>'
            
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
        
        # check if remote instance is a design instance, and create a bundle on it if so
        remote_design_instance = self.config.get("remote_design_instance")
        
        if remote_design_instance:
            create_bundle_on_remote_design_instance = self.config.get("create_bundle_on_remote_design_instance")
            if create_bundle_on_remote_design_instance:
                remote_project.export_bundle(bundle_id)
                html += '<div> Bundle Created on Remote Design Instance </div>'
        
        # if assigned a git remote to initial project, go back and delete it, and add back in initial git remotes
        if assign_git_remote:
            
            # read lines from initial file
            with open(path_to_git_remote_config, "r") as project_git_config:
                git_remote_config_lines = project_git_config.readlines()
            
            # write back only the lines from before adding the remote
            with open(path_to_git_remote_config, "w") as project_git_config:
                for line in git_remote_config_lines:
                    if line.strip("\n") not in git_remotes_config_lines['new']['line_1'] and line.strip("\n") not in git_remotes_config_lines['new']['line_2'] and line.strip("\n") not in git_remotes_config_lines['new']['line_3']:
                        project_git_config.write(line)
            
            html += '<div> Git Remote Deleted from Initial Design Instance </div>'
            
            # open the config file and append the previous git remote lines
            if len(remotes_list) > 0:
                with open(path_to_git_remote_config, "a") as project_git_config:
                    for existing_git_remote in git_remotes_config_lines['existing']:
                        
                        project_git_config.write('{}\n{}\n{}\n'.format(existing_git_remote['line_1'], existing_git_remote['line_2'], existing_git_remote['line_3']))
                html += '<div> Initial Git Remotes added back to Initial Design Instance </div>'
                
        html += '</div>'
        return html