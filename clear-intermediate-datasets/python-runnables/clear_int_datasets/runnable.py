import dataiku
import logging
import pandas as pd

from dataiku.runnables import Runnable, ResultTable
from utils import populate_result_table_with_list, append_datasets_to_list

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logging.getLogger().setLevel(logging.INFO)

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
        keep_partitioned = self.config.get("keep_partitioned")
        keep_shared = self.config.get("keep_shared")
        logging.info("DRY RUN is set to {}".format(str(is_dry_run)))
        logging.info("KEEP PARTITIONED is set to {}".format(str(keep_partitioned)))
        logging.info("KEEP SHARED is set to {}".format(str(keep_shared)))

        # Initialize macro result table:
        result_table = ResultTable()
        result_table.add_column("dataset", "Dataset", "STRING")
        result_table.add_column("type", "Type", "STRING")
        result_table.add_column("action", "Action", "STRING")
        result_table.add_column("action_status", "Action Status", "STRING")

        action_status = "Not done (Dry run)" if is_dry_run else "Done"
        
        client = dataiku.api_client()
        if self.config.get("project_key", None):
            project = client.get_project(self.config.get("project_key"))
        else:
            project = client.get_project(self.project_key)

        all_datasets = project.list_datasets()
        all_recipes = project.list_recipes()

        # Build deduplicated lists of input/output datasets:
        input_datasets = []
        output_datasets = []
        for recipe in all_recipes:
            recipe_inputs_dict = recipe["inputs"]
            recipe_outputs_dict = recipe["outputs"]
            # CASE: no input dataset
            if recipe_inputs_dict:
                append_datasets_to_list(recipe_inputs_dict, input_datasets)
            append_datasets_to_list(recipe_outputs_dict, output_datasets)

        # Identify Flow input/outputs:
        flow_inputs = [x for x in input_datasets if x not in output_datasets]
        flow_outputs = [x for x in output_datasets if x not in input_datasets]
        logging.info("Found {} FLOW INPUT datasets: {}".format(str(len(flow_inputs)),
                                                               str(flow_inputs)))
        logging.info("Found {} FLOW OUTPUT datasets: {}".format(str(len(flow_outputs)),
                                                                str(flow_outputs)))

        # Identify Intermediate datasets:
        intermediate_datasets = [x["name"] for x in all_datasets if x["name"] not in flow_inputs + flow_outputs]
        logging.info("Found {} INTERMEDIATE datasets: {}".format(str(len(intermediate_datasets)),
                                                                str(intermediate_datasets)))

        # Identify shared datasets:
        shared_objs = project.get_settings().settings["exposedObjects"]["objects"]
        shared_datasets = [x["localName"] for x in shared_objs if x["type"]=="DATASET"]
        logging.info("Found {} SHARED datasets: {}".format(str(len(shared_datasets)),
                                                           str(shared_datasets)))

        # Identify partitioned (partd) datasets:
        is_partd = lambda x: len(x["partitioning"]["dimensions"]) > 0
        partd_datasets = [x["name"] for x in all_datasets if is_partd(x)]
        logging.info("Found {} PARTITIONED datasets: {}".format(str(len(partd_datasets)),
                                                                str(partd_datasets)))

        # Add dataset types to results list
        results = []

        for obj in flow_inputs:
            results.append([obj, "INPUT"])

        for obj in flow_outputs:
            results.append([obj, "OUTPUT"])

        for obj in intermediate_datasets:
            results.append([obj, "INTERMEDIATE"])

        for obj in partd_datasets:
            results.append([obj, "PARTITIONED"])

        for obj in shared_datasets:
            results.append([obj, "SHARED"])

        # Identify which datasets should be kept
        to_keep = flow_inputs + flow_outputs
        if keep_partitioned:
            to_keep += partd_datasets
        if keep_shared:
            to_keep += shared_datasets
        logging.info("Total of {} datasets to KEEP: {}".format(str(len(to_keep)),
                                                               str(to_keep)))

        # Create df with all results
        results_df = pd.DataFrame(results, columns=["Dataset", "Type"])
        results_grouped = results_df.groupby(["Dataset"])['Type'].apply(lambda x: ', '.join(x)).reset_index()
        results_grouped["Action"] = results_grouped["Dataset"].apply(lambda x: "KEEP" if x in to_keep else "CLEAR")
        results_grouped["Status"] = action_status
        results_grouped = results_grouped.sort_values(by=['Action', 'Type'])

        # Perform cleanup
        to_clear = list(results_grouped["Dataset"][results_grouped['Action']=="CLEAR"])
        logging.info("Total of {} datasets to CLEAR: {}".format(str(len(to_clear)),
                                                               str(to_clear)))

        if not is_dry_run:
            for ds in to_clear:
                dataset = project.get_dataset(ds)
                logging.info("Clearing {}...".format(ds))
                dataset.clear()
            logging.info("Clearing {} datasets: done.".format(str(len(to_clear))))

        # Pass results to result table
        for index, row in results_grouped.iterrows():
            result_table.add_record(list(row))
        
        return result_table
