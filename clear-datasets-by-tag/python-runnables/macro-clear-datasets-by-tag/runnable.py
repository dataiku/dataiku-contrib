import dataiku
import logging

from dataiku.runnables import Runnable, ResultTable

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

def populate_result_table_with_list():
    # TODO
    # Populate result table from list with arbitrary table size
    return NotImplementedError()


class MyRunnable(Runnable):
    """The base interface for a Python runnable."""

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
        """
        If the runnable will return some progress info, have this function return a tuple of 
        (target, unit) where unit is one of: SIZE, FILES, RECORDS, NONE
        """

        return None

    def run(self, progress_callback):

        # Retrieve macro parameters:
        is_dry_run = self.config.get("is_dry_run")
        keep_shared = self.config.get("keep_shared")
        tag_to_drop = self.config.get("tag_to_drop")
        logging.info("DRY RUN is set to {}".format(str(is_dry_run)))
        logging.info("KEEP SHARED is set to {}".format(str(keep_shared)))

        # Initialize macro result table:
        result_table = ResultTable()
        result_table.add_column("dataset", "dataset", "STRING")
        result_table.add_column("status", "status", "STRING")

        client = dataiku.api_client()
        project = client.get_project(self.project_key)
        all_datasets = project.list_datasets()
        all_recipes = project.list_recipes()

        # Identify datasets to clear
        to_clear = []
        for dataset in all_datasets:
            dataset_object = project.get_dataset(dataset['name'])
            dataset_metadata = dataset_object.get_metadata()
            tags = dataset_metadata['tags']
            if tag_to_drop in tags:
                to_clear.append(dataset)

        # Identify shared datasets:
        shared_objs = project.get_settings().settings["exposedObjects"]["objects"]
        shared_datasets = [x["localName"] for x in shared_objs if x["type"]=="DATASET"]
        logging.info("Found {} SHARED datasets: {}".format(str(len(shared_datasets)),
                                                           str(shared_datasets)))

        # List datasets to keep:
        to_keep = []
        if keep_shared:
            to_keep += shared_datasets

        # List all datasets to clear
        cleared_datasets = [x["name"] for x in all_datasets if x in to_clear]
        for obj in cleared_datasets:
                result_table.add_record([obj, "CLEARED"])
        logging.info("Total of {} datasets to CLEAR: {}".format(str(len(to_clear)),
                                                               str(to_clear)))

        # Perform cleanup or simulate it (dry run):
        if not is_dry_run:
            for ds in to_clear:
                ds_name = ds["name"]
                if ds_name not in to_keep:
                    dataset = project.get_dataset(ds_name)
                    logging.info("Clearing {}...".format(ds_name))
                    dataset.clear()

        return result_table