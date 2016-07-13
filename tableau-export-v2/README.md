# Tableau export (v2) plugin

This plugin supersedes and deprecates the older "Tableau Export" plugin.

This plugin requires DSS 3.1 or higher

## Installation

Normally, you don't need to run any manual installation procedure.

### Manual installation

- Download the SDK for Python from http://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm#SDK/tableau_sdk_installing.htm
- Extract it, open a command prompt, and navigate to the directory that contains setup.py.
- Run ``DATA_DIR/bin/python setup.py install``

## Usage

The plugin adds two exporters to DSS:

* Export to TDE file (and download it)
* Export by uploading to a Tableau server

You can also use the "Export" recipe to automatically recreate TDE files.


## Changelog

* 0.0.1 - July 27th, 2016

	Initial release