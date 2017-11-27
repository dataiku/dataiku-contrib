#!/usr/bin/env python2

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'status': ['preview'],
    'supported_by': 'dataiku-contrib'
}

DOCUMENTATION = '''
---
module: dss_user

short_description: Creates, edit or delete a Data Science Studio user 

description:
    - "This module edits a complete user profile. If the user does not exist and is required to, it is created. If the user exists but is supposed not to, it deleted"

options:
    host:
        description:
            - The host when DSS is installed
        required: true
    port:
        description:
            - The port on which DSS listens
        default: 80
        required: false
    api_key:
        description:
            - The API_KEY to authenticate onto the DSS API
        required: true
    login:
        description:
            - The login name of the user
        required: true
    password:
        description:
            - The unencrypted password of the user. Mandatory if the user must be created
        required: false
    set_password_at_creation_only:
        description:
            - Allow not to change the password to the requested one if the user already exists. This is
              the only way to actually achieve idempotency, so it is true by default. If set to false, the task
              will always have the "changed" status because we cannot check if the password was actually different before
              or not
        default: true
        required: false
    email:
        description:
            - The email of the user
        required: false
    display_name:
        description:
            - The display name for the user. Defaults to the login at creation.
        required: false
    groups:
        description:
            - The list of groups the user belongs to. If not set at creation, defaults to ["readers"]
        default: empty
        required: false
    profile:
        description:
            - The profile type of the user. Mandatory if the user must be created
        default: READER
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
from dataikuapi.dss.admin import DSSUser
from dataikuapi.utils import DataikuException
import copy
import traceback

# Tricj to expose dictionary as python args
class MakeNamespace(object):
    def __init__(self,values):
        self.__dict__.update(values)

def run_module():
    # define the available arguments/parameters that a user can pass to
    # the module
    module_args = dict(
        host=dict(type='str', required=False, default="localhost"),
        port=dict(type='str', required=False, default="80"),
        api_key=dict(type='str', required=True, no_log=True),
        login=dict(type='str', required=True),
        password=dict(type='str', required=False, default=None, no_log=True),
        set_password_at_creation_only=dict(type='bool', required=False, default=True),
        email=dict(type='str', required=False, default=None),
        display_name=dict(type='str', required=False, default=None),
        groups=dict(type='list', required=False, default=None),
        profile=dict(type='str', required=False, default=None),
        state=dict(type='str', required=False, default="present"),
        )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    args = MakeNamespace(module.params)
    if args.state not in ["present","absent"]:
        module.fail_json(msg="Invalid value '{}' for argument state : must be either 'present' or 'absent'".format(args.state))

    result = dict(
        changed=False,
        original_message=args.login,
        message='UNCHANGED'
    )

    client = DSSClient("http://{}:{}".format(args.host, args.port),api_key=args.api_key)
    user = DSSUser(client, args.login)
    try:
        user_exists = True
        create_user = False
        current_user = None
        try:
            current_user = user.get_definition()
        except DataikuException as e:
            if e.message.startswith("com.dataiku.dip.server.controllers.NotFoundException"):
                user_exists = False
                if args.state == "present":
                    create_user = True
            else:
                raise
        except:
            raise

        # Manage errors
        if args.password is None and create_user:
            module.fail_json(msg="The 'password' parameter is missing but is mandatory to create new user '{}'.".format(args.login))
        if args.display_name is None and create_user:
            #module.fail_json(msg="The 'display_name' parameter is missing but is mandatory to create new user '{}'.".format(args.login))
            # TODO: shall we fail here or use a default to login ?
            args.display_name = args.login
            module.params["display_name"] = args.login

        # Build the new user definition
        # TODO: be careful that the key names changes between creation and edition
        new_user_def = copy.deepcopy(current_user) if user_exists else {} # Used for modification
        for key, api_param in [("email","email"),("display_name","displayName"),("profile","userProfile"),("groups","groups")]:
            if module.params.get(key,None) is not None:
                new_user_def[key if create_user else api_param] = module.params[key]
        if user_exists and args.password is not None and not args.set_password_at_creation_only:
            new_user_def["password"] = args.password

        # Sort groups list before comparison as they should be considered sets
        new_user_def.get("groups",['readers'] if create_user else []).sort()
        new_user_def["groups"] = ["readers"]
        if user_exists:
            current_user.get("groups",[]).sort()

        # Prepare the result for dry-run mode
        result["changed"] = create_user or (user_exists and args.state == "absent") or (user_exists and current_user != new_user_def)
        if result["changed"]:
            if create_user:
                result["message"] = "CREATED"
            elif user_exists:
                if  args.state == "absent":
                    result["message"] = "DELETED"
                elif current_user != new_user_def:
                    result["message"] = "MODIFIED"

        if module.check_mode:
            module.exit_json(**result)

        # Apply the changes
        if result["changed"]:
            if create_user:
                client.create_user(args.login, args.password, **new_user_def)
            elif user_exists:
                if args.state == "absent":
                    user.delete()
                elif current_user != new_user_def:
                    result["message"] = str(user.set_definition(new_user_def))

        module.exit_json(**result)
    except Exception as e:
        module.fail_json(msg=str(e))

def main():
    run_module()

if __name__ == '__main__':
    main()
