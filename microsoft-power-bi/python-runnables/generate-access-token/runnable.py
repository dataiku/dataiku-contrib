# This file is the actual code for the Python runnable generate-access-token
import os
import sys
import json
import dataiku
import requests
from powerbi import *
from dataiku.runnables import Runnable


class PowerBIAccesTokenGenerator(Runnable):

    
    def __init__(self, project_key, config, plugin_config):
        self.config = config
        self.project_key = project_key
        self.plugin_config = plugin_config
        self.username        = self.config.get("username",      None)
        self.password        = self.config.get("password",      None)
        self.client_id       = self.config.get("client-id",     None)
        self.client_secret   = self.config.get("client-secret", None)
        
    def run(self, progress_callback):
        response = generate_access_token(
            self.username,
            self.password,
            self.client_id,
            self.client_secret
        )
        token = response.get("access_token")
        if token is None:
            print "ERROR can't retrieve an access token. Please check your credentials"
            sys.exit("Authentication error")
        # Project Variables    
        dss = dataiku.api_client()
        project = dss.get_project( self.project_key )
        variables = project.get_variables()
        if variables["standard"].get("powerbi-settings") is None:
            variables["standard"]["powerbi-settings"] = {}
        variables["standard"]["powerbi-settings"]["access_token"] = token
        variables["standard"]["powerbi-settings"]["project_key"] = self.project_key
        project.set_variables(variables)
        return "Successfully generated a new access token for Power BI."
            
            
            
        