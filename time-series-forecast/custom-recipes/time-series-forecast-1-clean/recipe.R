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
checkPartitioning <- CheckPartitioningSettings(inputDatasetName,
  PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)

selectedColumns <- c(TIME_COLUMN, SERIES_COLUMNS)
columnClasses <- c("character", rep("numeric", length(SERIES_COLUMNS)))
dfInput <- dkuReadDataset(inputDatasetName, columns = selectedColumns, colClasses = columnClasses) 


########## DATA PREPARATION STAGE ##########

PrintPlugin("Data preparation stage starting...")

dfOutput <- dfInput %>%
  PrepareDataframeWithTimeSeries(TIME_COLUMN, SERIES_COLUMNS, 
  	GRANULARITY, AGGREGATION_STRATEGY) %>%
  CleanDataframeWithTimeSeries(TIME_COLUMN, SERIES_COLUMNS, GRANULARITY, 
    MISSING_VALUES, MISSING_IMPUTE_WITH, MISSING_IMPUTE_CONSTANT, 
    OUTLIERS, OUTLIERS_IMPUTE_WITH, OUTLIERS_IMPUTE_CONSTANT)

PrintPlugin("Data preparation stage completed, saving prepared data to output dataset.")

WriteDatasetWithPartitioningColumn(dfOutput, outputDatasetName, 
  PARTITION_DIMENSION_NAME, checkPartitioning)

PrintPlugin("All stages completed!")