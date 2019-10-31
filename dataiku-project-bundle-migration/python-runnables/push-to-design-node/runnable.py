import sys
import dataiku
import dataikuapi
from dataiku.runnables import Runnable


class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.html = ''
        self.remote_host = None
        self.new_project_key = None
        self.replace_project = None
        self.ignore_ssl_certs = None
        self.activate_scenarios = None
        self.local_client = None
        self.remote_client = None
        self.project = None
        self.delete_project_first = False
        
    def get_progress_target(self):
        return None

    def run(self, progress_callback):

        self.set_params()
        if self.ignore_ssl_certs:
            self.remote_client._session.verify = False
        self.check_remote_project()
        self.check_remote_connection()
        self.update_remote_code_env()
        self.import_project()
        self.check_remote_plugin()
        if self.activate_scenarios:
            self.activate_remote_scenario()
        return self.html

    def set_params(self):
        self.remote_host = self.config.get("remote_host", "")
        if self.remote_host == "":
            raise Exception("destination is required")

        api_key = self.config.get("api_key", "")
        if api_key == "":
            raise Exception("API key is required")

        self.new_project_key = self.config.get("new_project_key", self.project_key)
        self.replace_project = self.config.get("replace_project", False)
        self.ignore_ssl_certs = self.config.get("ignore_ssl_certs", False)
        self.activate_scenarios = self.config.get("activate_scenarios", False)
        self.local_client = dataiku.api_client()
        self.remote_client = dataikuapi.DSSClient(self.remote_host, api_key)
        self.project = self.local_client.get_project(self.project_key)
        self.html += '<div> Successfully connected to remote host: {}</div>'.format(self.remote_client.host)

    def check_remote_project(self):
        try:
            remote_projects = self.remote_client.list_project_keys()
        except:
            remote_projects = []

        if self.project_key in remote_projects:
            if not self.replace_project:
                self.html += '<div> Project {} already exists on instance {}.  Nothing to do.</div>'.format(self.new_project_key, self.remote_host)
                return self.html
            else:
                self.html += '<div> Project {} already exists on instance {}.  Overwriting it ...</div>'.format(self.new_project_key, self.remote_host)
                self.delete_project_first = True
        else:
            self.html += '<div> Project {} does not exist on instance {}. Creating it ...</div>'.format(self.project_key, self.remote_host)

    def check_remote_connection(self):
        # get list of connections used in the initial project
        datasets = self.project.list_datasets()
        connections_used = []
        for dataset in datasets:
            connection = dataset.get('params', {}).get('connection')
            if connection is not None:
                connections_used.append(dataset['params']['connection'])

        connections_used = list(set(connections_used))

        # get list of connections in remote project
        remote_connections_list = self.remote_client.list_connections()
        remote_connections_names = remote_connections_list.keys()

        # check if connections used in the initial project also exist on the remote instance
        for connection in connections_used:
            if connection not in remote_connections_names:
                error_msg = 'Failed - Connection {} used in initial project does not exist on instance {}.'.format(self.connection, self.remote_host)
                raise Exception(error_msg)
        self.html += '<div><div>All connections exist on both instances.</div>'

    def update_remote_code_env(self):
        # check for code-env, create if not exist
        code_envs_used = []
        already_noted_list = [None]
        for recipe in self.project.list_recipes():
            env_name = recipe.get('params', {}).get('envSelection', {}).get('envName', None)
            env_type = recipe.get('type')
            if env_name not in already_noted_list:
                code_envs_used.append({'env_name': env_name, 'env_lang': env_type})
                already_noted_list.append(env_name)

        for code_env_detail in code_envs_used:
            env_lang = code_env_detail.get('env_lang')
            env_name = code_env_detail.get('env_name')
            local_code_env = self.local_client.get_code_env(env_lang, env_name)
            already_exist = env_name in [env.get('envName') for env in self.remote_client.list_code_envs()]

            if not already_exist:
                if env_lang == 'python':
                    python_interpreter = local_code_env.get_definition().get('desc').get('pythonInterpreter')
                    if python_interpreter == 'CUSTOM':
                        raise ValueError('Code-env {} uses a custom interpreter, can not recreate it automatically on the remote instance.'.format(env_name))
                    _ = self.remote_client.create_code_env(env_lang=env_lang, env_name=env_name, deployment_mode='DESIGN_MANAGED', params={'pythonInterpreter': python_interpreter})
                else:  # R
                    _ = self.remote_client.create_code_env(env_lang=env_lang, env_name=env_name, deployment_mode='DESIGN_MANAGED')
                self.html += '<div>Code env "{}" is created.</div>'.format(env_name)

            remote_code_env = self.remote_client.get_code_env(env_lang, env_name)
            env_def = remote_code_env.get_definition()
            env_def['specPackageList'] = local_code_env.get_definition().get('specPackageList', '')
            env_def['desc']['installCorePackages'] = True
            remote_code_env.set_definition(env_def)
            remote_code_env.update_packages()

    def check_remote_plugin(self):
        original_plugin_names = {}
        for plugin in self.local_client.list_plugins():
            original_plugin_names[plugin['meta']['label']] = plugin['version']

        # compare to list of plugins in remote instance
        remote_plugin_names = {}
        for plugin in self.remote_client.list_plugins():
            remote_plugin_names[plugin['meta']['label']] = plugin['version']

        missing_plugins = {k: original_plugin_names[k] for k in original_plugin_names if
                           k not in remote_plugin_names or original_plugin_names[k] == remote_plugin_names[k]}

        if len(missing_plugins) > 0:
            self.html += '<div> <b> Warning: the following plugins (and versions) are installed on this instance, but not on the remote instance. Please ensure that your project does not use these plugins before proceeding. </b> </div>'
            self.html += '<table>'
            self.html += '<tr>'
            self.html += '<th>Plugin</th>'
            self.html += '<th>Version</th>'
            self.html += '</tr>'

        for plugin in missing_plugins:
            self.html += '<tr>'
            self.html += '<td> {} </td>'.format(plugin)
            self.html += '<td> {} </td>'.format(missing_plugins[plugin])
            self.html += '</tr>'

        if len(missing_plugins) > 0:
            self.html += '</table>'

    def import_project(self):

        if self.delete_project_first:
            remote_project = self.remote_client.get_project(self.new_project_key)
            remote_project.delete()

        with self.project.get_export_stream() as s:
            import_handler = self.remote_client.prepare_project_import(s)
            try:
                res = import_handler.execute(settings={})
                success = res.get('success')
            except Exception as e:
                from future.utils import raise_
                raise_(Exception, "Fail to import project.", sys.exc_info()[2])

        if success:
            self.html += '<div>Migration succeeded.</div>'.format(success)
        else:
            self.html += '<div>Migration failed: </div>'
            for message in res.get('messages'):
                if message.get('severity') == 'ERROR':
                    self.html += '<div> &nbsp; {}</div>'.format(message.get('details'))
        self.html += '</div>'

    def activate_remote_scenario(self):
        # activate scenarios that were active on design instance
        project_active_scenarios = []
        for scenario in self.project.list_scenarios():
            s = self.project.get_scenario(scenario['id'])
            scenario_def = s.get_definition()
            if scenario_def['active']:
                project_active_scenarios.append(scenario_def['id'])

        for active_scenario_id in project_active_scenarios:
            remote_project = self.remote_client.get_project(self.new_project_key)
            scenario = remote_project.get_scenario(active_scenario_id)
            scenario_def = scenario.get_definition()
            scenario_def['active'] = True
            scenario.set_definition(scenario_def)