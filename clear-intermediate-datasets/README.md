# Clear Intermediate Datasets Macro

## Description
Save disk space by clearing intermediate datasets in the flow.

## CAUTION
The following macro clears intermediate datasets, which can result in the irreversible loss of data.
Always run this macro in dry-run mode first (as enabled by default) and check in the output list to 
confirm that no dataset is unintendedly cleared.

### Partitioned and shared datasets
Partitioned and shared datasets are particularly sensitive. You can select the available options to keep your partitioned and shared datasets in the flow. 
them from the list of datasets to be cleared, if desired so.

### Dependencies
Any of the following datas associated to a dataset can be affected when clearing it:
- Charts will be cleared
- Insights published in a dashboard will be cleared
- Insights shared in a workspace will be cleared
- Scenario with the dataset as input of a scenario step will break 
