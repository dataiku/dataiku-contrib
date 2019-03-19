Current Version: 0.0.2
Compatible with DSS version: 5.0.2

**For earlier versions of DSS please refer to the manual installation procedure below**

## Plugin information

[Tableau](https://tableau.com) is one of the leading data visualization tools

The recently released [Hyper format](https://www.tableau.com/about/blog/2018/1/onboarding-your-team-hyper-79398) deprecates former Tableau Extract API and Server SDK

This plugin offers the ability to export Dataiku data to Tableau with the Hyper format:

* Export to a Hyper file for immediate open in Tableau Desktop
* Directly upload a dataset to Tableau Server with the Hyper format

## Prerequisites

The plugin comes prepackaged with a code environement that will install the [Tableau Server Client](https://tableau.github.io/server-client-python/)

The plugin also depends on [Tableau Extract API 2.0](https://onlinehelp.tableau.com/current/api/extract_api/en-us/help.htm)

Tableau Extract API 2.0 and Tableau Extract API 1.0 **cannot** coexist in the same environement.

For DSS version 5.0.2 and later, an script is provided that will automatically download and install the SDK into the code environement.

For DSS **prior** to version **5.0.2** you will need to run Tableau extract API 2.0 **manually into the code environement of the plugin**

## How it works

Once the plugin is successfully installed you can use it a regular DSS exporter

Please refer to the [Dataiku Plugin webpage](https://www.dataiku.com/dss/plugins/info/tableau-hyper-extract.html) for detailed usage information

## Manual Installation

Tableau Server Client will be installed in the plugin code environement automatically since it is accessible through pip

To install the Tableau Extract API 2.0 manually (_e.g: for DSS versions prior to 5.0.2_) you will need to:

* [Download](https://onlinehelp.tableau.com/current/api/extract_api/en-us/Extract/extract_api_installing.htm#downloading) the SDK manually on the DSS Server machine
* Untar and decompress the package
* Run the installation setup using the plugin codenvironement python binary

```
$DIP_HOME/code-envs/python/plugin_tableau-hyper-export_managed/bin/python setup.py build
$DIP_HOME/code-envs/python/plugin_tableau-hyper-export_managed/bin/python setup.py install
```

where $DIP_HOME stands for your DSS home directory

