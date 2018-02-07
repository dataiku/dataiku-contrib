from dataiku.runnables import Runnable, ResultTable

import os, json, copy

class CheckAnalysisDataRunnable(Runnable):

    def __init__(self, project_key, config, plugin_config):
        self.project_key = project_key
        self.config = config
        self.plugin_config = plugin_config
        
    def get_progress_target(self):
        return None
    
    def disk_space_used(self, p):
        total_size = 0
        for dir_path, dir_names, file_names in os.walk(p):
            for file_name in file_names:
                total_size += os.path.getsize(os.path.join(dir_path, file_name))
        return total_size

    def run(self, progress_callback):
        dip_home = os.environ["DIP_HOME"]
        config_home = os.path.join(dip_home, "config")
        
        analysis_data_folder = os.path.join(dip_home, "analysis-data")

        rt = ResultTable()
        rt.set_name("Analysis data")
        rt.add_column("projectKey", "Project", "STRING")
        rt.add_column("dataset", "Dataset", "STRING")
        rt.add_column("analysis", "Analysis", "STRING")
        rt.add_column("model", "Model", "STRING")
        rt.add_column("used", "Disk space used", "STRING")
        rt.add_column("path", "Path", "STRING")
 
        for project_key in os.listdir(analysis_data_folder):
            analysis_data_project_folder = os.path.join(analysis_data_folder, project_key)
            if not os.path.isdir(analysis_data_project_folder):
                continue
                
            project_folder = os.path.join(config_home, "projects", project_key)
            orphaned_project = not os.path.isdir(project_folder)
            
            for analysis_id in os.listdir(analysis_data_project_folder):
                analysis_data_analysis_folder = os.path.join(analysis_data_project_folder, analysis_id)
                if not os.path.isdir(analysis_data_analysis_folder):
                    continue
                
                analysis_folder = os.path.join(project_folder, "analysis", analysis_id)
                orphaned_analysis = not os.path.isdir(analysis_folder) if not orphaned_project else None

                total_used = 0
                
                model_records = []
                for model_id in os.listdir(analysis_data_analysis_folder):
                    analysis_data_model_folder = os.path.join(analysis_data_analysis_folder, model_id)
                    if not os.path.isdir(analysis_data_model_folder):
                        continue

                    model_folder = os.path.join(analysis_folder, "ml", model_id)
                    orphaned_model = not os.path.isdir(model_folder) if not orphaned_project and not orphaned_analysis else None

                    try:
                        used = self.disk_space_used(analysis_data_model_folder)
                        total_used += used
                    except:
                        used = None
                        
                    try:
                        core_params_file = os.path.join(analysis_folder, "core_params.json")
                        if os.path.isfile(core_params_file):
                            with open(core_params_file, 'r') as f:
                                core_params = json.load(f)
                            dataset_name = core_params.get('inputDatasetSmartName', None)
                            analysis_name = core_params.get('name', None)
                        else:
                            dataset_name = None
                            analysis_name = None
                    except:
                        dataset_name = None
                        analysis_name = None
                    
                    try:
                        model_params_file = os.path.join(model_folder, "params.json")
                        if os.path.isfile(model_params_file):
                            with open(model_params_file, 'r') as f:
                                model_params = json.load(f)
                            model_name = model_params.get('name', None)
                        else:
                            model_name = None
                    except:
                        model_name = None
                    
                    record = []
                    
                    # 0
                    if orphaned_project:
                        record.append('(orphaned)')
                    else:
                        record.append(project_key)
                    
                    # 1
                    record.append(dataset_name)
                    
                    # 2
                    if orphaned_analysis:
                        record.append('(orphaned)')
                    elif analysis_name is not None:
                        record.append(analysis_name)
                    else:
                        record.append(analysis_id)

                    # 3
                    if orphaned_model:
                        record.append('(orphaned)')
                    elif model_name is not None:
                        record.append(model_name)
                    else:
                        record.append(model_id)
                    
                    # 4
                    if used is None:
                        record.append('N/A')
                    elif used < 1024:
                        record.append('%s b' % used)
                    elif used < 1024 * 1024:
                        record.append('%s Kb' % int(used/1024))
                    elif used < 1024 * 1024 * 1024:
                        record.append('%s Mb' % int(used/(1024*1024)))
                    else:
                        record.append('%s Gb' % int(used/(1024*1024*1024)))
                    
                    # 5
                    record.append(analysis_data_model_folder)
                    
                    model_records.append(record)
                    
                record = []

                # 0
                if orphaned_project:
                    record.append('(orphaned)')
                else:
                    record.append(project_key)

                # 1
                record.append(dataset_name)

                # 2
                if orphaned_analysis:
                    record.append('(orphaned)')
                elif analysis_name is not None:
                    record.append(analysis_name)
                else:
                    record.append(analysis_id)
                
                # 3
                record.append(None)

                # 4
                if total_used is None:
                    record.append('N/A')
                elif total_used < 1024:
                    record.append('%s b' % used)
                elif total_used < 1024 * 1024:
                    record.append('%s Kb' % int(total_used/1024))
                elif total_used < 1024 * 1024 * 1024:
                    record.append('%s Mb' % int(total_used/(1024*1024)))
                else:
                    record.append('%s Gb' % int(total_used/(1024*1024*1024)))

                # 5
                record.append(analysis_data_analysis_folder)

                rt.add_record(record)
                for model_record in model_records:
                    rt.add_record(model_record)
                    
                    
        table_rows = []
        idx = 0
        for record in rt.records:
            analysis_row = record[3] is None

            row_cells = []
            for i in range(0, 6):
                if analysis_row and i == 3:
                    continue
                value = record[i]
                if value is not None:
                    if i == 5:
                        show_path_var = "showPath%s" % idx
                        row_cells.append('<td class="mx-textellipsis" title="%s"><a class="mx-link-nodecoration" href="" ng-click="%s = !%s"><i class="icon-eye"></i></a></td>' % (value, show_path_var, show_path_var))
                    else:
                        row_cells.append('<td class="mx-textellipsis" title="%s" %s>%s</td>' % (value, (' colspan="2"' if analysis_row and i == 2 else ''), value))
                else:
                    row_cells.append('<td></td>')
                    
            if analysis_row:
                # analysis row
                table_rows.append('<tr style="font-weight: bold;">%s</tr>' % (''.join(row_cells)))
            else:
                # model row
                table_rows.append('<tr>%s</tr>' % (''.join(row_cells)))
            path_cell_style = 'white-space: nowrap; padding-left: 20px; font-family: monospace; font-size: 11px;'
            if analysis_row:
                path_cell_style = path_cell_style + '; font-weight: bold'
            table_rows.append('<tr ng-if="%s"><td colspan="6" title="%s" style="%s">%s</td></tr>' % (show_path_var, record[5], path_cell_style, record[5]))
            idx += 1
                
        html = '<div>'
        table_header = '<th>%s</th>' % ('</th><th>'.join(['Project', 'Dataset', 'Analysis', 'Model', 'Disk usage', 'Path']))
        html += '<table class="table table-striped" style="table-layout: fixed;">%s%s</table>' % (table_header, ''.join(table_rows))
        html += '</div>'
        return html
        # return rt
                    
                                                
                    
                    
                    
                    
                    
                    
                    