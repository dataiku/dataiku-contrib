import subprocess, os, os.path as osp
def du(directory, size_unit="k"):
    """disk usage in kilobytes"""
    return int(subprocess.check_output(['du','-s{}'.format(size_unit), directory]).split()[0].decode('utf-8'))

def get_projects_to_consider(current_project_key, config):
    mode =  config.get('projectsMode', "CURRENT")

    if mode == "ALL_BUT_IGNORED":
        dip_home = os.environ["DIP_HOME"]
        config_projects = osp.join(dip_home, "config", "projects")
        all_projects = set(os.listdir(config_projects))
        ignored = set([x.strip() for x in config.get("ignoredProjects", "").split(",")])
        projects = list(all_projects - ignored)
    elif mode == "INCLUDED":
        projects  = list(set([x.strip() for x in config.get("includedProjects", "").split(",")]))
    elif mode == "CURRENT":
        projects = [current_project_key]
    else:
        raise Exception("Unexpected projects mode: %s" % mode)

    return projects

def format_size(size):
    if size is None:
        return 'N/A'
    elif size < 1024:
        return '%s b' % size
    elif size < 1024 * 1024:
        return '%s Kb' % int(size/1024)
    elif size < 1024 * 1024 * 1024:
        return '%s Mb' % int(size/(1024*1024))
    else:
        return '%s Gb' % int(size/(1024*1024*1024))
