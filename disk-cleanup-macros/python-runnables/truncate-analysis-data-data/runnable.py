from dataiku.runnables import Runnable, ResultTable
from dataiku import pandasutils as pdu
import dataiku
import pandas as pd
import os, shutil, os.path as osp
import datetime, time
import json
import cleanup
import glob
import logging

class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        
    def get_progress_target(self):
        return None

    def run(self, progress_callback):
        maximum_age = int(self.config.get("age",  15))  
        maximum_timestamp = int(time.mktime((datetime.datetime.now() - datetime.timedelta(days=maximum_age)).timetuple()))
        lines       = int(self.config.get("lines", 5))
        orphans_only = bool(self.config.get("orphansOnly", False))
        do_it = bool(self.config.get("performDeletion", False))
        dip_home = os.environ['DIP_HOME']
        config_home = os.path.join(dip_home, "config")
        analysis_data_folder = osp.join(dip_home, "analysis-data")

        def truncate_file(path, rows):
            yourfile = pd.read_csv(path, nrows = rows)
            yourfile.to_csv(path, index = False)

        rt = ResultTable()
        rt.set_name("Saved models cleanup")
        rt.add_column("project", "Project key", "STRING")
        rt.add_column("analysis", "Analysis", "STRING")
        rt.add_column("dataset", "Dataset", "STRING")
        rt.add_column("model", "Model", "STRING")
        rt.add_column("total_size_before", "Total space before", "STRING")
        rt.add_column("total_size_after", "Total space after", "STRING")
        rt.add_column("kept_splits", "Kept splits", "STRING")
        rt.add_column("truncated_splits", "Truncated splits", "STRING")
        rt.add_column("reclaimed_size", "Reclaimed size", "STRING")
        
        grand_total_used = 0
        grand_total_reclaimed = 0
        grand_total_kept = 0
        grand_total_deleted = 0
        for project_key in cleanup.get_projects_to_consider(self.project_key, self.config):
            analysis_data_project_folder = osp.join(analysis_data_folder, project_key)
            if not osp.isdir(analysis_data_project_folder):
                continue
            project_folder = os.path.join(config_home, "projects", project_key)
            
            for analysis_id in os.listdir(analysis_data_project_folder):
                analysis_data_analysis_folder = osp.join(analysis_data_project_folder,analysis_id)
                if not osp.isdir(analysis_data_analysis_folder):
                    continue

                analysis_folder = os.path.join(project_folder, "analysis", analysis_id)
                total_used = 0
                total_reclaimed = 0
                total_kept = 0
                total_deleted = 0
                model_records = []
                dataset_name = None
                analysis_name = None
                try:
                    core_params_file = os.path.join(analysis_folder, "core_params.json")
                    if os.path.isfile(core_params_file):
                        with open(core_params_file, 'r') as f:
                            core_params = json.load(f)
                            dataset_name = core_params.get('inputDatasetSmartName', None)
                            analysis_name = core_params.get('name', None)
                except Exception:
                    pass

                model_ids = []
                for model_id in os.listdir(analysis_data_analysis_folder):
                    analysis_data_model_folder = os.path.join(analysis_data_analysis_folder, model_id)
                    if not osp.isdir(analysis_data_model_folder):
                        continue

                    model_folder = os.path.join(analysis_folder, "ml", model_id)
                    used = 0
                    reclaimed = 0
                    kept = 0
                    deleted = 0
                    try:
                        used = cleanup.du(analysis_data_model_folder,size_unit="b")
                    except Exception:
                        pass

                    model_name = None
                    try:
                        model_params_file = os.path.join(model_folder, "params.json")
                        if os.path.isfile(model_params_file):
                            with open(model_params_file, 'r') as f:
                                model_params = json.load(f)
                            model_name = model_params.get('name', None)
                    except:
                        pass
                    
                    splits_dates = {}

                    # Scan session to find out split usage
                    sessions_folder = os.path.join(analysis_data_model_folder,"sessions")
                    if osp.isdir(sessions_folder):
                        for session in os.listdir(sessions_folder):
                            split_ref_file = osp.join(sessions_folder, session, "split_ref.json")
                            if not osp.isfile(split_ref_file):
                                continue
                            session_timestamp = os.stat(osp.join(sessions_folder, session)).st_mtime
                            split_ref = None
                            with open(split_ref_file, 'r') as f:
                                split_ref = json.load(f).get("splitInstanceId",None)
                            if split_ref is not None and splits_dates.get(split_ref,0) < session_timestamp:
                                splits_dates[split_ref] = session_timestamp
                          
                    # Check it agaisnt actual splits
                    splits_folder = os.path.join(analysis_data_model_folder,"splits")
                    if osp.isdir(splits_folder):
                        for split in glob.glob(osp.join(splits_folder,"*.json")):
                            split_name, _ = osp.splitext(split)
                            split_short_name = osp.basename(split_name)
                            split_date = splits_dates.get(split_short_name, None)
                            if split_date is None or (split_date < maximum_timestamp and not orphans_only):
                                deleted += 1
                                split_data = {}
                                with open(split, 'r') as f:
                                    split_data = json.load(f)
                                for split_data_filename in [split_data.get("testPath",None), split_data.get("trainPath",None)]:
                                    if split_data_filename is None:
                                        continue
                                    split_data_file = osp.join(splits_folder,split_data_filename)
                                    _, split_data_extension = osp.splitext(split)
                                    if osp.isfile(split_data_file):
                                        if do_it:
                                            if split_date is None:
                                                reclaimed = os.stat(split_data_file).st_size 
                                                os.unlink(split_data_file)
                                            else:
                                                size_before = os.stat(split_data_file).st_size 
                                                try:
                                                    data_file = pd.read_csv(split_data_file, nrows = lines)
                                                    data_file.to_csv(split_data_file, index = False, compression="gzip" if split_data_extension == "gz" else None)
                                                except Exception as e:
                                                    logging.getLogger().error("{}: {}".format(split_data_file,str(e)))
                                                reclaimed = size_before - os.stat(split_data_file).st_size 
                                            pass
                                    else:
                                        reclaimed = os.stat(split_data_file).st_size
                                if do_it and split_date is None:
                                    os.unlink(split)
                                    pass
                            else:
                                kept += 1

                    total_reclaimed += reclaimed
                    total_used += used
                    total_kept += kept
                    total_deleted += deleted
                    
                    model_records.append([
                        project_key,
                        analysis_name,
                        dataset_name,
                        model_name,
                        cleanup.format_size(used), 
                        cleanup.format_size(used-reclaimed),
                        kept,
                        deleted,
                        cleanup.format_size(reclaimed)
                        ])

                rt.add_record([
                    project_key,
                    analysis_name,
                    dataset_name,
                    "Total all models",
                    cleanup.format_size(total_used), 
                    cleanup.format_size(total_used-total_reclaimed),
                    total_kept,
                    total_deleted,
                    cleanup.format_size(total_reclaimed)
                    ])
                for record in model_records:
                    rt.add_record(record)

                grand_total_reclaimed += total_reclaimed
                grand_total_used += total_used
                grand_total_kept += total_kept
                grand_total_deleted += total_deleted

        rt.add_record([
            "Total used",
            "-",
            "-",
            "-",
            cleanup.format_size(grand_total_used), 
            cleanup.format_size(grand_total_used-grand_total_reclaimed),
            grand_total_kept,
            grand_total_deleted,
            cleanup.format_size(grand_total_reclaimed)
            ])

        return rt
