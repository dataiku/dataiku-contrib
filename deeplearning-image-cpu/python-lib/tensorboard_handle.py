import dataiku
import constants
from dataikuapi.utils import DataikuException
import os
from threading import Thread
from werkzeug.serving import make_server
from tensorflow import logging
from tensorboard.backend import application
import tensorboard.default as tb_default

class TensorboardThread(Thread):

    def __init__(self, folder_name, host="127.0.0.1", verbosity=logging.WARN):
        Thread.__init__(self)
        self.project_key = os.environ["DKU_CURRENT_PROJECT_KEY"]
        self.folder_name = folder_name
        self.client = dataiku.api_client()

        logging.set_verbosity(verbosity)

        # Getting app
        logs_path = self.__get_logs_path()
        app = self.__get_tb_app(logs_path)

        # Setting server
        self.srv = make_server(host, 0, app)

    def get_port(self):
        return self.srv.server_port

    def __get_logs_path(self):
        # Retrieve model managed-folder path
        folder_found = False
        project = self.client.get_project(self.project_key)
        for folder in project.list_managed_folders():
            if self.folder_name == folder['name']:
                folder_path = dataiku.Folder(folder['id'], project_key=self.project_key).get_path()
                folder_found = True
                break

        if not folder_found:
            raise DataikuException("The folder '{}' (in project '{}' cannot be found".format(self.folder_name, self.project_key))

        log_path = os.path.join(folder_path, constants.TENSORBOARD_LOGS)
        return log_path

    def __get_tb_app(self, tensorboard_logs):
        return application.standard_tensorboard_wsgi(
              logdir=tensorboard_logs,
              assets_zip_provider=tb_default.get_assets_zip_provider(),
              purge_orphaned_data=True,
              reload_interval=5,
              plugins=tb_default.get_plugins())

    def run(self):
        print("Launching tensorboard :")
        print("Your tensorboard dashboard will be accessible on http://<SERVER ADDRESS>:{}".format(self.get_port()))
        self.srv.serve_forever()

    def stop(self):
        print("Stopping tensorboard process")
        self.srv.shutdown()





