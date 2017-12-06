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

author:
    - Jean-Bernard Jansen (jean-bernard.jansen@dataiku.com)
'''

EXAMPLES = '''
'''

RETURN = '''
original_message:
    description: The original login name
    type: str
message:
    description: CREATED, MODIFIED or DELETED 
    type: str
'''

from ansible.module_utils.basic import AnsibleModule
from dataikuapi import DSSClient
from dataikuapi.dss.admin import DSSGroup
from dataikuapi.utils import DataikuException
import copy
import traceback

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
        source_type=dict(type='str', required=False, default="LOCAL"),
        state=dict(type='str', required=False, default="present"),
        )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    args = MakeNamespace(module.params)
    if args.state not in ["present","absent"]:
        module.fail_json(msg="Invalid value '{}' for argument state : must be either 'present' or 'absent'".format(args.source_type))
    if args.source_type not in ["LOCAL","LDAP","SAAS"]:
        module.fail_json(msg="Invalid value '{}' for source_type : must be either 'LOCAL', 'LDAP' or 'SAAS'".format(args.state))
    api_key = args.api_key if args.api_key is not None else args.connect_to.get("api_key",None)
    if api_key is None:
        module.fail_json(msg="Missing an API Key, either from 'api_key' or 'connect_to' parameters".format(args.state))
    port = args.port if args.port is not None else args.connect_to.get("port","80")
    host = args.host

    result = dict(
        changed=False,
        original_message=args.name,
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

        result["group_def"] = current
        module.exit_json(**result)

        # Build the new user definition
        # TODO: be careful that the key names changes between creation and edition
        new_def = copy.deepcopy(current_user) if user_exists else {} # Used for modification
        #for key, api_param in [("email","email"),("display_name","displayName"),("profile","userProfile"),("groups","groups")]:
            #if module.params.get(key,None) is not None:
                #new_user_def[key if create_user else api_param] = module.params[key]
        #if user_exists and args.password is not None and not args.set_password_at_creation_only:
            #new_user_def["password"] = args.password

        # Sort groups list before comparison as they should be considered sets
        #new_user_def.get("groups",[]).sort()
        #if user_exists:
            #current_user.get("groups",[]).sort()

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

        if module.check_mode:
            module.exit_json(**result)

        # Apply the changes
        if result["changed"]:
            if create:
                client.create_group(args.login, args.name, description = new_def["description"], source_type=new_def["source_type"])
                # 2nd request mandatory for capabilites TODO: fix the API
                # group.set_definition(new_def)
            elif exists:
                if args.state == "absent":
                    group.delete()
                elif current_user != new_user_def:
                    result["message"] = str(group.set_definition(new_def))

        module.exit_json(**result)
    except Exception as e:
        module.fail_json(msg=str(e))

def main():
    run_module()

if __name__ == '__main__':
    main()
