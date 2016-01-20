Installation
---
- Download the SDK for Python from http://onlinehelp.tableau.com/current/api/sdk/en-us/help.htm#SDK/tableau_sdk_installing.htm
- Extract it, open a command prompt, and navigate to the directory that contains setup.py.
- Run DATA_DIR/bin/python setup.py install

Usage
---
Make sure to choose a folder (not a dataset) as output. Note this folder is emptied when the recipe starts (like most recipes, the output is overwritten at each run).

Known limitation
---
Partitioned input is not supported (the partition id would be dropped). Workaround:
- run a sync recipe from your partitioned input into a non partitioned dataset
- take the latter as input for tde export.

TDE does not support partitioning anyways and requires the result to be one single .tde file.
