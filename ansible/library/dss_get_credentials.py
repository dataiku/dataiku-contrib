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
    datadir:
        description:
            - The datadir where DSS is installed. Be mindful to become the applicative user to call this module.
        required: true
    api_key_name:
        description:
            - The name of the api key to look for.
        required: false
        default: "dss-ansible-admin"

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
        datadir=dict(type='str', required=True),
        api_key_name=dict(type='str', required=False, default="dss-ansible-admin"),
    )

    module = AnsibleModule(
        argument_spec=module_args,
        supports_check_mode=True
    )

    args = MakeNamespace(module.params)

    result = dict(
        changed=False,
        dss_credentilas={},
    )

    try:
        module.exit_json(**result)
    except Exception as e:
        module.fail_json(msg=str(e))

def main():
    run_module()

if __name__ == '__main__':
    main()
