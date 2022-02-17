# Clear Intermediate Datasets Macro

## Description
Save disk space by clearing intermediate datasets in the flow.

## CAUTION!
The following macro clears intermediate datasets, which can result in the irreversible loss of data.
Always run this macro in dry-run mode first (as enabled by default) and check in the output list to 
confirm that no dataset is unintendedly cleared.

### Partitioned and shared datasets
Partitioned and shared datasets are particularly sensitive. You can select the available options to keep your partitioned and shared datasets in the flow. 
them from the list of datasets to be cleared, if desired so.

### Dependencies
Any of the following assets associated to a dataset can be affected when clearing it:
#### Visualizations
These won't be available or appeared broken until the source dataset is rebuilt again:
- Charts
- Insights published in a dashboard
- Insights shared to a workspace
#### Scenarios
Scenario with the dataset as input of a scenario step can break. E.g:
- Usign a custom python step that reads the cleared dataset as input
- Usign a schema propagation step that starts from a cleared dataset
