from dataiku.runnables import Runnable, ResultTable
from dataiku import pandasutils as pdu
import dataiku
import pandas as pd
import os, shutil, os.path as osp
import datetime, time
import json
import cleanup
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

        do_it = bool(self.config.get("performDeletion", False))

        dip_home = os.environ['DIP_HOME']
        saved_models = osp.join(dip_home, 'saved_models')

        def truncate_file(path, rows):
            yourfile = pd.read_csv(path, nrows = rows)
            yourfile.to_csv(path, index = False)

        rt = ResultTable()
        rt.set_name("Saved models cleanup")
        rt.add_column("project", "Project key", "STRING")
        rt.add_column("saved_model_id", "Saved model id", "STRING")
        rt.add_column("saved_model_name", "Saved model name", "STRING")
        rt.add_column("total_size_before", "Total space before (MB)", "STRING")
        rt.add_column("total_size_after", "Total space after (MB)", "STRING")
        rt.add_column("kept_splits", "Kept splits", "STRING")
        rt.add_column("truncated_splits", "Truncated splits", "STRING")
        rt.add_column("reclaimed_size", "Reclaimed size", "STRING")

        grand_total_used = 0
        grand_total_reclaimed = 0
        grand_total_kept = 0
        grand_total_deleted = 0
        
        for project in cleanup.get_projects_to_consider(self.project_key, self.config):
            project_sm = osp.join(saved_models, project)

            if not osp.isdir(project_sm):
                continue

            for saved_model in os.listdir(project_sm):
                sm_dir = osp.join(project_sm, saved_model)
                versions_dir = osp.join(sm_dir, "versions")

                if not osp.isdir(versions_dir):
                    continue

                kept_versions = 0
                deleted_versions = 0
                size_reclaimed = 0
                total_size_before = cleanup.du(sm_dir, size_unit="b")

                for version in os.listdir(versions_dir):
                    version_dir = osp.join(versions_dir, version)

                    if os.stat(version_dir).st_mtime < maximum_timestamp:
                        # Need to clean this version
                        deleted_versions += 1
                        split_dir = osp.join(version_dir, "split")
                        if osp.isdir(split_dir):
                            for name in os.listdir(split_dir):
                                path = osp.join(split_dir, name)
                                ext = osp.splitext(path)[-1].lower()
                                if ext == ".csv":
                                    if do_it:
                                        try:
                                            initial = os.stat(path).st_size 
                                            truncate_file(path, lines)
                                            size_reclaimed += initial - os.stat(path).st_size
                                        except Exception as e:
                                            logging.getLogger().error("{}: {}".format(path,str(e)))
                                    else:
                                        size_reclaimed += os.stat(path).st_size
                    else:
                        kept_versions += 1
                    
                total_size_after = cleanup.du(sm_dir,size_unit="b")
                record = []
                record.append(project)
                record.append(saved_model)
                record.append(saved_model)
                record.append(cleanup.format_size(total_size_before))
                record.append(cleanup.format_size(total_size_after))
                record.append(kept_versions)
                record.append(deleted_versions)
                record.append(cleanup.format_size(size_reclaimed))
                rt.add_record(record)

                grand_total_reclaimed += size_reclaimed
                grand_total_used += total_size_before
                grand_total_kept += kept_versions
                grand_total_deleted += deleted_versions

        rt.add_record([
            "Total",
            "-",
            "-",
            cleanup.format_size(grand_total_used), 
            cleanup.format_size(grand_total_used-grand_total_reclaimed),
            grand_total_kept,
            grand_total_deleted,
            cleanup.format_size(grand_total_reclaimed)
            ])
        return rt
