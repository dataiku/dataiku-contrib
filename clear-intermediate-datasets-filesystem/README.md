# Clear Intermediate Datasets by Filesystem Type Macro

## Description
Save disk space by clearing intermediate datasets in the flow.

## CAUTION
The following macro clears intermediate datasets that are filesystem type, which can result in the irreversible loss of data.
Always run this macro in dry-run mode first (as enabled by default) and check in the output list to 
confirm that no dataset is unintendedly cleared.

### Partitioned and shared datasets
Partitioned and shared datasets are particularly sensitive. Selecting the available options _Keep partitioned datasets_ and _Keep shared datasets_ will ensure that they are not cleared. 

### Dependencies
Any of the following assets associated to a dataset can be affected when clearing it:
#### Visualizations
The following won't be available or will appear broken until the source dataset is rebuilt again:
- Charts
- Insights published in a dashboard
- Insights shared to a workspace
#### Scenarios
Scenario with the dataset as input of a scenario step can break. E.g:
- Using a custom python step that reads the cleared dataset as input
- Using a schema propagation step that starts from a cleared dataset
