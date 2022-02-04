# Clear Intermediate Datasets Macro

## CAUTION
The following macro clears intermediate datasets, which can result in the irreversible loss of data.
Always run this macro in dry-run mode first (as enabled by default) and check in the output list to 
confirm that no dataset is unintendedly cleared.

Partitioned and Shared datasts are paticularly sensitive, and specific options has been added to remove
them from the list of datasets to be cleared, if desired so.

Be mindful of dependencies as well. Any of the follwing can be affected when clearing a dataset that contains:
- Charts
- Insights published in a dashboard
- Insights shared in a workspace,
- It's explicitily linked to an scenario step
