library(dataiku)

# Read dataset names
input_dataset <- dkuCustomRecipeInputNamesForRole("main")
output_dataset <- dkuCustomRecipeOutputNamesForRole("main")

# Read configuration
config <- dkuCustomRecipeConfig()
sort_column <- config[["column"]]

# Read input
inp <- dkuReadDataset(input_dataset)

# Sort it
sorted <- inp[with(inp, order(inp[sort_column])), ]

# Write it
dkuWriteDataset(sorted, output_dataset)