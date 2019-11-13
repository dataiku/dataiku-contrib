import sys
import dataiku
import dataikuapi
from dataiku.runnables import Runnable
import logging

logger = logging.getLogger(__name__)


class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.html = ''
        self.remote_host = None
        self.bundle_id = None
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
        self.import_project()
        self.check_remote_plugin()
        if self.activate_scenarios:
            self.activate_remote_scenario()
        return self.html

    def set_params(self):

        self.bundle_id = self.config.get("bundle_id", "")
        if self.bundle_id == "":
            raise Exception("Bundle id is required")
        self.remote_host = self.config.get("remote_host", "")
        if self.remote_host == "":
            raise Exception("destination is required")

        api_key = self.config.get("api_key", "")
        if api_key == "":
            raise Exception("API key is required")

        self.ignore_ssl_certs = self.config.get("ignore_ssl_certs", False)
        self.activate_scenarios = self.config.get("activate_scenarios", False)
        self.local_client = dataiku.api_client()
        self.remote_client = dataikuapi.DSSClient(self.remote_host, api_key)
        self.project = self.local_client.get_project(self.project_key)

        self.html += '<div> Successfully connected to remote host: {0}</div>'.format(self.remote_client.host)
        logger.info('Successfully connected to remote host: {0}'.format(self.remote_client.host))

    def check_remote_project(self):
        # check if there are any projects in remote instance
        try:
            remote_projects = self.remote_client.list_project_keys()
        except:
            remote_projects = []

        if not self.project_key in remote_projects:
            self.remote_client.create_project(self.project_key, self.project_key, 'admin', 'placeholder description')
            self.html += '<div> Project {0} does not exist on instance {1}. Creating it.</div>'.format(self.project_key, self.remote_host)
            logger.info('Project {0} does not exist on instance {1}. Creating it.'.format(self.project_key, self.remote_host))
        else:
            self.html += '<div> Project {0} already exists on instance {1}.  Updating with new bundle version {2}.</div>'.format(self.project_key, self.remote_host, self.bundle_id)
            logger.info('Project {0} already exists on instance {1}.  Updating with new bundle version {2}.</div>'.format(self.project_key, self.remote_host, self.bundle_id))

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
                error_msg = 'Failed - Connection {0} used in initial project does not exist on instance {1}.'.format(self.connection, self.remote_host)
                raise Exception(error_msg)
        self.html += '<div><div>All connections exist on both instances.</div>'
        logger.info('All connections exist on both instances.')

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
            self.html += '<td> {0} </td>'.format(plugin)
            self.html += '<td> {0} </td>'.format(missing_plugins[plugin])
            self.html += '</tr>'

        if len(missing_plugins) > 0:
            self.html += '</table>'

    def import_project(self):

        # create the bundle (verify that the bundle_id does not exist)
        try:
            self.project.export_bundle(self.bundle_id)
            self.html += '<div><div>Successfully created bundle: {0}</div>'.format(self.bundle_id)
            logger.info('Successfully created bunde: {0}'.format(self.bundle_id))
        except Exception as e:
            from future.utils import raise_
            raise_(Exception, "Fail to create bundle: {}".format(str(e)), sys.exc_info()[2])

        # check if there are any projects in remote instance
        try:
            remote_projects = self.remote_client.list_project_keys()
        except:
            remote_projects = []

        if self.project_key in remote_projects:
            self.html += '<div> Project {0} already exists on instance {1}.  Updating with new bundle version {2}.</div>'.format(self.project_key, self.remote_host, self.bundle_id)
            logger.info('Project {0} already exists on instance {1}.  Updating with new bundle version {2}.'.format(self.project_key, self.remote_host, self.bundle_id))
        else:
            self.remote_client.create_project(self.project_key, self.project_key, 'admin', 'placeholder description')
            self.html += '<div> Project {0} does not exist on instance {1}. Creating it.</div>'.format(self.project_key, self.remote_host)
            logger.info('Project {0} does not exist on instance {1}. Creating it.'.format(self.project_key, self.remote_host))

        # connect to remote project
        remote_project = self.remote_client.get_project(self.project_key)

        with self.project.get_exported_bundle_archive_stream(self.bundle_id) as fp:
            try:
                remote_project.import_bundle_from_stream(fp.content)
            except Exception as e:
                from future.utils import raise_
                raise_(Exception, "Fail to import bundle: {}".format(str(e)), sys.exc_info()[2])

        # "preload bundle" to create/update custom code environments used throughout the project
        _ = remote_project.preload_bundle(self.bundle_id)

        # activate bundle
        remote_project.activate_bundle(self.bundle_id)
        self.html += '<div><vr>Bundle activated</br></div>'
        self.info('Bundle activated.')

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
