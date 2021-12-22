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
        for rcp in all_recipes:
            rcp_inputs_dict = rcp["inputs"]
            rcp_outputs_dict = rcp["outputs"]
            # CASE: no input dataset
            if rcp_inputs_dict:
                input_key = list(rcp_inputs_dict.keys())[0]
                rcp_inputs_list = [x["ref"] for x in rcp_inputs_dict[input_key]["items"]]
                input_datasets += rcp_inputs_list
            output_key = list(rcp_outputs_dict.keys())[0]
            rcp_outputs_list = [x["ref"] for x in rcp_outputs_dict[output_key]["items"]]
            # Append them to the overall input list:
            output_datasets += rcp_outputs_list
        # Deduplicate input/output lists:
        input_datasets = list(set(input_datasets))
        output_datasets = list(set(output_datasets))

        # Identify Flow input/outputs & add them to result table:
        flow_inputs = [x for x in input_datasets if x not in output_datasets]
        for obj in flow_inputs:
            result_table.add_record([obj, "INPUT", "KEEP", action_status])
        flow_outputs = [x for x in output_datasets if x not in input_datasets]
        for obj in flow_outputs:
            result_table.add_record([obj, "OUTPUT", "KEEP", action_status])
        logging.info("Found {} FLOW INPUT datasets: {}".format(str(len(flow_inputs)),
                                                               str(flow_inputs)))
        logging.info("Found {} FLOW OUTPUT datasets: {}".format(str(len(flow_outputs)),
                                                                str(flow_outputs)))

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

        # List all datasets to keep, potentially including shared & partd ones:
        to_keep = flow_inputs + flow_outputs
        if keep_partitioned:
            to_keep += partd_datasets
            # Add them to result table:
            for obj in partd_datasets:
                result_table.add_record([obj, "PARTITIONED", "KEEP", action_status])
        if keep_shared:
            to_keep += shared_datasets
            # Add them to result table:
            for obj in shared_datasets:
                result_table.add_record([obj, "SHARED", "KEEP", action_status])
        logging.info("Total of {} datasets to KEEP: {}".format(str(len(to_keep)),
                                                               str(to_keep)))

        # Perform cleanup or simulate it (dry run):
        to_clear = []
        for ds in all_datasets:
            if ds["name"] not in to_keep:
                result_table.add_record([ds["name"], "INTERMEDIATE", "CLEAR", action_status])
                to_clear.append(ds["name"])
        logging.info("Total of {} datasets TO CLEAR: {}".format(str(len(to_clear)),
                                                               str(to_clear)))
        if not is_dry_run:
            for ds in to_clear:
                dataset = project.get_dataset(ds)
                logging.info("Clearing {}...".format(ds))
                dataset.clear()

        return result_table
