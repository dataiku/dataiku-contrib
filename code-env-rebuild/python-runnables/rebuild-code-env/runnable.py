from dataiku.runnables import Runnable, ResultTable
import dataiku
import time
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,  # avoid getting log from 3rd party module
                    format='code-envs-rebuild-macro %(levelname)s - %(message)s')


class MyRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):

        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        self.client = dataiku.api_client()
        self.plugin_managed_envs = [c for c in self.client.list_code_envs() if c.get('deploymentMode') == 'PLUGIN_MANAGED']
        
    def get_progress_target(self):

        return (len(self.plugin_managed_envs), "RECORDS")

    def run(self, progress_callback):

        rt = ResultTable()
        rt.add_column("code_env", "Code-env", "STRING")
        rt.add_column("status", "Status", "STRING")
        
        for index, c in enumerate(self.plugin_managed_envs):
            
            env_name = c.get('envName')
            env_lang = c.get('envLang')

            progress_callback(index+1)
            record = []
            record.append(env_name)
            
            logger.info('Rebuilding {} ...'.format(env_name))
            
            try:
                #save the config and drop the old code-env
                old_env = self.client.get_code_env(env_lang, env_name)
                env_def = old_env.get_definition()  
                old_env.delete()

                #rebuild the code-env
                new_env = self.client.create_code_env(env_lang=env_lang, 
                                                     env_name=env_name, 
                                                     deployment_mode='PLUGIN_MANAGED',
                                                     params=env_def.get('desc'))
                new_env.set_definition(env_def)
                new_env.update_packages()
                
                record.append('Success')
                rt.add_record(record)
                
            except Exception as e:
                record.append('Failed: {}'.format(e)) #TODO: so we lost the code-env, what now ? ...
                rt.add_record(record)
        
        return rt