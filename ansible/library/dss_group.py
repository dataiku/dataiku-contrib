#!/usr/bin/env python2

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'dataiku-contrib'
}

DOCUMENTATION = '''
---
module: dss_user

short_description: Creates, edit or delete a Data Science Studio group

description:
    - "This module edits a complete group. If the group does not exist and is required to, it is created. If the group exists but is supposed not to, it is deleted"

options:
    connect_to:
        description:
            - A dictionary containing "port" and "api_key". This parameter is a short hand to be used with dss_get_credentials
        required: true
    host:
        description:
            - The host on which to make the requests.
        required: false
        default: localhost
    port:
        description:
            - The port on which to make the requests.
        required: false
        default: 80
    api_key:
        description:
            - The API Key to authenticate on the API. Mandatory if connect_to is not used
        required: false
    name:
        description:
            - Name of the group
        required: true
    description:
        description:
            - Description of the group
    source_type:
        description:
            - The source type of the group, either LOCAL, LDAP or SAAS
        required: false
    state:
        description:
            - Wether the user is supposed to exist or not. Possible values are "present" and "absent"
        default: present
        required: false
    admin:
        description:
            - Tells if the group has administration credentials
        default: false
        required: false
    ldap_group_names:
        description:
            - Desc
        default: false
        required: false
    may_create_authenticated_connections:
        description:
            - Desc
        default: false
        required: false
    may_create_code_envs:
        description:
            - Desc
        default: false
        required: false
    may_create_projects:
        description:
            - Desc
        default: false
        required: false
    may_develop_plugins:
        description:
            - Desc
        default: false
        required: false
    may_edit_lib_folders:
        description:
            - Desc
        default: false
        required: false
    may_manage_code_envs:
        description:
            - Desc
        default: false
        required: false
    may_manage_u_d_m:
        description:
            - Desc
        default: false
        required: false
    may_view_indexed_hive_connections:
        description:
            - Desc
        default: false
        required: false
    may_write_safe_code:
        description:
            - Desc
        default: false
        required: false
    may_write_unsafe_code:
        description:
            - Desc
        default: false
        required: false

author:
    - Jean-Bernard Jansen (jean-bernard.jansen@dataiku.com)
'''

EXAMPLES = '''
# Creates a group using dss_get_credentials if you have SSH Access
- name: Get the API Key
  become: true
  become_user: dataiku
  dss_get_credentials:
    datadir: /home/dataiku/dss
    api_key_name: myadminkey
  register: dss_connection_info
- name: Add a group
  become: true
  become_user: dataiku
  dss_group:
    connect_to: "{{dss_connection_info}}"
    name: dssgroup

# Creates a group using explicit host/port/key
# From local machine
- name: Add a user  
  delegate_to: localhost
  dss_user:
    host: 192.168.0.2
    port: 80
    api_key: XXXXXXXXXXXXXX
    name: dssgroup 

# Deletes a group
- name: Add a user  
  become: true
  become_user: dataiku
  dss_user:
    connect_to: "{{dss_connection_info}}"
    group: dssgroup
    state: absent
'''

RETURN = '''
previous_group_def:
    description: The previous values
    type: dict
group_def:
    description: The current values is the group have not been deleted
    type: dict
message:
    description: CREATED, MODIFIED, UNCHANGED or DELETED 
    type: str
'''

from ansible.module_utils.basic import AnsibleModule
from dataikuapi import DSSClient
from dataikuapi.dss.admin import DSSGroup
from dataikuapi.utils import DataikuException
import copy
import traceback
import re
import time

# Trick to expose dictionary as python args
class MakeNamespace(object):
    def __init__(self,values):
        self.__dict__.update(values)

def run_module():
    # define the available arguments/parameters that a user can pass to
    # the module
    module_args = dict(
        connect_to=dict(type='dict', required=False, default={}, no_log=True),
        host=dict(type='str', required=False, default="127.0.0.1"),
        port=dict(type='str', required=False, default=None),
        api_key=dict(type='str', required=False, default=None),
        name=dict(type='str', required=True),
        description=dict(type='str', required=False, default=None),
        source_type=dict(type='str', required=False, default=None),
        state=dict(type='str', required=False, default="present"),
        admin=dict(type='bool', required=False, default=None),
        ldap_group_names=dict(type='list', required=False, default=None),
        may_create_authenticated_connections=dict(type='bool', required=False, default=None),
        may_create_code_envs=dict(type='bool', required=False, default=None),
        may_create_projects=dict(type='bool', required=False, default=None),
        may_develop_plugins=dict(type='bool', required=False, default=None),
        may_edit_lib_folders=dict(type='bool', required=False, default=None),
        may_manage_code_envs=dict(type='bool', required=False, default=None),
        may_manage_u_d_m=dict(type='bool', required=False, default=None),
        may_view_indexed_hive_connections=dict(type='bool', required=False, default=None),
        may_write_safe_code=dict(type='bool', required=False, default=True),
        may_write_unsafe_code=dict(type='bool', required=False, default=None),
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    args = MakeNamespace(module.params)
    if args.state not in ["present","absent"]:
        module.fail_json(msg="Invalid value '{}' for argument state : must be either 'present' or 'absent'".format(args.source_type))
    if args.source_type not in [None,"LOCAL","LDAP","SAAS"]:
        module.fail_json(msg="Invalid value '{}' for source_type : must be either 'LOCAL', 'LDAP' or 'SAAS'".format(args.state))
    api_key = args.api_key if args.api_key is not None else args.connect_to.get("api_key",None)
    if api_key is None:
        module.fail_json(msg="Missing an API Key, either from 'api_key' or 'connect_to' parameters".format(args.state))
    port = args.port if args.port is not None else args.connect_to.get("port","80")
    host = args.host

    result = dict(
        changed=False,
        message='UNCHANGED',
    )

    client = DSSClient("http://{}:{}".format(args.host, port),api_key=api_key)
    group = DSSGroup(client, args.name)
    try:
        exists = True
        create = False
        current = None
        try:
            current = group.get_definition()
        except DataikuException as e:
            if e.message.startswith("com.dataiku.dip.server.controllers.NotFoundException"):
                exists = False
                if args.state == "present":
                    create = True
            else:
                raise
        except:
            raise


        # Sort groups list before comparison as they should be considered sets
        if exists:
            current["ldapGroupNames"]= ",".join(sorted(current.get("ldapGroupNames","").split(",")))
            result["previous_group_def"] = current
        # Build the new user definition
        new_def = copy.deepcopy(current) if exists else {} # Used for modification

        # Transform to camel case
        dict_args = {}
        if args.ldap_group_names is not None:
            dict_args["ldapGroupNames"] = ",".join(sorted(args.ldap_group_names))
        for key, value in module.params.items():
            if key not in ["connect_to","host","port","api_key","state","ldap_group_names"] and value is not None:
                camelKey = re.sub(r'_[a-z]', lambda x : x.group()[1:].upper(), key)
                dict_args[camelKey] = value
        new_def.update(dict_args)

        # Prepare the result for dry-run mode
        result["changed"] = create or (exists and args.state == "absent") or (exists and current != new_def)
        if result["changed"]:
            if create:
                result["message"] = "CREATED"
            elif exists:
                if  args.state == "absent":
                    result["message"] = "DELETED"
                elif current != new_def:
                    result["message"] = "MODIFIED"

        if args.state == "present":
            result["group_def"] = new_def

        if module.check_mode:
            module.exit_json(**result)

        # Apply the changes
        if result["changed"]:
            if create:
                new_group = client.create_group(args.name, description = new_def.get("description",None), source_type=new_def.get("source_type","LOCAL"))
                # 2nd request mandatory for capabilites TODO: fix the API
                if "mayWriteSafeCode" not in new_def.keys():
                    new_def["mayWriteSafeCode"] = True
                new_group.set_definition(new_def)
                result["group_def"] = new_group.get_definition()
            elif exists:
                if args.state == "absent":
                    group.delete()
                elif current != new_def:
                    result["message"] = str(group.set_definition(new_def))

        module.exit_json(**result)
    except Exception as e:
        module.fail_json(msg="{}: {}".format(type(e).__name__,str(e)))

def main():
    run_module()

if __name__ == '__main__':
    main()
