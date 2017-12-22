#!/usr/bin/env python2

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'dataiku-contrib'
}

DOCUMENTATION = '''
---
module: dss_user

short_description: Creates/get an admin API key onto a DSS datadir.

description:
    - This module reads a datadir and returns the port on which the studio is exposed as well as an admin API Key.

options:
    datadir:
        description:
            - The datadir where DSS is installed. Be mindful to become the applicative user to call this module.
        required: true
    api_key_name:
        description:
            - The name of the api key to look for. No effect for now.
        required: false
        default: "dss-ansible-admin"

author:
    - Jean-Bernard Jansen (jean-bernard.jansen@dataiku.com)
'''

EXAMPLES = '''
# Creates and displays a key with a label
- name: Get the API Key
  become: true
  become_user: dataiku
  dss_get_credentials:
    datadir: /home/dataiku/dss
    api_key_name: mytestkey
  register: dss_connection_info
- name: Debug
  debug:
    var: dss_connection_info
'''

RETURN = '''
port:
    description: The port on which DSS is exposed
    type: str
api_key:
    description: An admin valid API Key
    type: str
'''

from ansible.module_utils.basic import AnsibleModule
import copy
import traceback
import os
import ConfigParser
import imp
import logging
import subprocess
import json

# Tricj to expose dictionary as python args
class MakeNamespace(object):
    def __init__(self,values):
        self.__dict__.update(values)

def run_module():
    # define the available arguments/parameters that a user can pass to
    # the module
    module_args = dict(
        datadir=dict(type='str', required=True),
        api_key_name=dict(type='str', required=False, default="dss-ansible-admin"),
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    args = MakeNamespace(module.params)


    try:
        if not os.path.isdir(args.datadir):
            module.fail_json(msg="Datadir '{}' not found.".format(args.datadir))
        
        current_uid = os.getuid()
        current_datadir_uid = os.stat(args.datadir).st_uid
        if current_uid != current_datadir_uid:
            module.fail_json(msg="The dss_get_credentials MUST be ran as the owner of the datadir (ran as UID={}, datadir owned by UID={})".format(current_uid, current_datadir_uid))

        # Setup the log
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', filename="{}/run/ansible.log".format(args.datadir),filemode="a")

        # Read the port
        config = ConfigParser.RawConfigParser()
        config.read("{}/install.ini".format(args.datadir))
        port =  str(config.getint("server","port")) 
        nodetype = config.get("general","nodetype").strip()
        logging.info("Reads port {} from install.ini".format(port))

        # Create/Get the api key 
        changed = False
        api_key = None
        exec_name = "apinode-admin" if nodetype == "api" else "dsscli"
        api_keys_list = json.loads(subprocess.check_output(["{}/bin/{}".format(args.datadir,exec_name),"admin-keys-list" if nodetype == "api" else "api-keys-list","--output","json"]))
        for key in api_keys_list:
            if key["label"] == args.api_key_name:
                api_key = key["key"]
                if not module.check_mode:
                    logging.info("Found existing API Key labeled \"{}\".".format(args.api_key_name))
                break
        if api_key is None:
            if not module.check_mode:
                command = [
                    "{}/bin/{}".format(args.datadir,exec_name),
                    "admin-key-create" if nodetype == "api" else "api-key-create",
                    "--output","json",
                    "--label", args.api_key_name,
                ]
                if nodetype != "api":
                    command += ["--admin","true"]
                api_keys_list = json.loads(subprocess.check_output(command))
                if nodetype == "api":
                    api_key = api_keys_list["key"]
                else:
                    api_key = api_keys_list[0]["key"]
                logging.info("Created new API Key labeled \"{}\".".format(args.api_key_name))
            changed = True
        
        # Build result
        result = dict(
            changed=changed,
            port=port,
            api_key=api_key,
        )

        module.exit_json(**result)
    except Exception as e:
        module.fail_json(msg=str(e))

def main():
    run_module()

if __name__ == '__main__':
    main()
