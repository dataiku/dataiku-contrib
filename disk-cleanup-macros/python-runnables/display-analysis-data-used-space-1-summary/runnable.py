from dataiku.runnables import Runnable, ResultTable
import dataiku
import subprocess
import os, os.path as osp
import cleanup

class MyRunnable(Runnable):
    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        
    def get_progress_target(self):
        return (100, 'NONE')

    def run(self, progress_callback):
        dip_home = os.environ['DIP_HOME']
        analysis_data = osp.join(dip_home, 'analysis-data')

        projects_sessions = {}
        projects_splits = {}
        analyses_sessions = {}
        analyses_splits = {}
        projects_analyses = {}

        if self.config.get('allProjects', False):
            projects = [project_key for project_key in os.listdir(analysis_data)]
        else:
            projects = [self.project_key]

        for project in projects:
            project_analysis_data = osp.join(analysis_data, project)
            project_sessions = 0
            project_splits = 0
            projects_analyses[project] = []

            if not osp.isdir(project_analysis_data):
                projects_sessions[project] = 0
                projects_splits[project] = 0
                continue

            for analysis in os.listdir(project_analysis_data):
                analysis_dir = osp.join(project_analysis_data, analysis)
                analysis_sessions = 0
                analysis_splits = 0
                projects_analyses[project].append(analysis)

                for mltask in os.listdir(analysis_dir):
                    mltask_dir = osp.join(analysis_dir, mltask)
                    sessions_dir = osp.join(mltask_dir, "sessions")
                    splits_dir = osp.join(mltask_dir, "splits")

                    if osp.isdir(sessions_dir):
                        analysis_sessions += cleanup.du(sessions_dir)
                    if osp.isdir(splits_dir):
                        analysis_splits += cleanup.du(splits_dir)

                project_sessions += analysis_sessions
                project_splits += analysis_splits

                analyses_splits[(project, analysis)] = analysis_splits
                analyses_sessions[(project, analysis)] = analysis_sessions
        
            projects_sessions[project] = project_sessions
            projects_splits[project] = project_splits

        rt = ResultTable()
        rt.set_name("Analysis data used space")

        if self.config["granularity"] == "project":
            rt.add_column("project", "Project key", "STRING")
            rt.add_column("total", "Total space (MB)", "STRING")
            rt.add_column("sessions", "Sessions space (MB)", "STRING")
            rt.add_column("splits", "Splits space (MB)", "STRING")

            for project in projects:
                total = (projects_sessions[project] + projects_splits[project])
                if len(projects) > 0 and total == 0:
                    continue
                record = []
                record.append(project)
                record.append(total / 1024)
                record.append(projects_sessions[project] / 1024)
                record.append(projects_splits[project] / 1024)
                rt.add_record(record)
        else:
            rt.add_column("project", "Project key", "STRING")
            rt.add_column("analysis", "Analysis id", "STRING")
            rt.add_column("total", "Total space (MB)", "STRING")
            rt.add_column("sessions", "Sessions space (MB)", "STRING")
            rt.add_column("splits", "Splits space (MB)", "STRING")

            for project in projects:
                for analysis in projects_analyses[project]:
                    record = []
                    record.append(project)
                    record.append(analysis)
                    record.append((analyses_sessions[(project, analysis)]+analyses_splits[(project, analysis)])/ 1024)
                    record.append(analyses_sessions[(project, analysis)] / 1024)
                    record.append(analyses_splits[(project, analysis)] / 1024)
                    rt.add_record(record)

        return rt