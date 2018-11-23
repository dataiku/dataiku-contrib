########## LIBRARY LOADING ##########

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "clean.R"))


########## INPUT OUTPUT CONFIGURATION ##########

inputDatasetName = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
outputDatasetName = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)
for(n in names(config)) {
  assign(n, CleanPluginParam(config[[n]]))
}
   
# Check that partitioning settings are correct if activated
checkPartition <- CheckPartitioningSettings(inputDatasetName,
  PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)

selectedColumns <- c(TIME_COLUMN, SERIES_COLUMNS)
columnClasses <- c("character", rep("numeric", length(SERIES_COLUMNS)))


########## DATA PREPARATION STAGE ##########

dfInput <- dkuReadDataset(inputDatasetName, columns = selectedColumns, colClasses = columnClasses) 

  # replace outlier and or missing values WARNING: heavy computational load
dfOutput <- CleanDataframeWithTimeSeries(dfInput, TIME_COLUMN, SERIES_COLUMNS, GRANULARITY, 
    MISSING_VALUES, MISSING_IMPUTE_WITH, MISSING_IMPUTE_CONSTANT, 
    OUTLIERS, OUTLIERS_IMPUTE_WITH, OUTLIERS_IMPUTE_CONSTANT)

# Recipe outputs
WriteDatasetWithPartitioningColumn(dfOutput, outputDatasetName, 
  PARTITION_DIMENSION_NAME, checkPartitioning)