# Acquiring an access token for the Azure Power BI Plugin

**Date**: June 30th, 2019

## Overview
In order to use the Power BI Plugin, you will need a proper access token to interact with the service through thier REST API's. 
The current version of the Plugin offers utilities to acquire valid credentials but this can prove challenging to get all the steps working properly. 
This document shows a procedure to acquire an access token providing with the proper credentials to use the Plugin and push a Dataiku dataset into a Power BI dataset.

For an high-level perspective, the steps involve are the following:	
* (optional) create a "service" Azure user and assign the required settings 
* register an Application that will interact with the Power BI service on behalf of the user above - and set the proper permissions
* obtain an access token and make it available to the Plugin

## Create a "service" user that will interact with the Power BI service
