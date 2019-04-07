##### LIBRARY LOADING #####

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "clean.R"))


##### INPUT OUTPUT CONFIGURATION #####

INPUT_DATASET_NAME <- dkuCustomRecipeInputNamesForRole('INPUT_DATASET_NAME')[1]
OUTPUT_DATASET_NAME <- dkuCustomRecipeOutputNamesForRole('OUTPUT_DATASET_NAME')[1]

config = dkuCustomRecipeConfig()
for(n in names(config)) {
  assign(n, CleanPluginParam(config[[n]]))
}

# Check that partitioning settings are correct if activated
checkPartitioning <- CheckPartitioningSettings(INPUT_DATASET_NAME)

selectedColumns <- c(TIME_COLUMN, SERIES_COLUMNS)
columnClasses <- c("character", rep("numeric", length(SERIES_COLUMNS)))
dfInput <- dkuReadDataset(INPUT_DATASET_NAME, columns = selectedColumns, colClasses = columnClasses)


##### DATA PREPARATION STAGE #####

PrintPlugin("Data preparation stage starting...")

dfOutput <- dfInput %>%
  PrepareDataframeWithTimeSeries(TIME_COLUMN, SERIES_COLUMNS,
    GRANULARITY, AGGREGATION_STRATEGY) %>%
  CleanDataframeWithTimeSeries(TIME_COLUMN, SERIES_COLUMNS, GRANULARITY,
    MISSING_VALUES, MISSING_IMPUTE_WITH, MISSING_IMPUTE_CONSTANT,
    OUTLIERS, OUTLIERS_IMPUTE_WITH, OUTLIERS_IMPUTE_CONSTANT)

if (nrow(dfOutput) > 3 * nrow(dfInput)) {
  PrintPlugin(paste0("Resampled data is 3 times longer than input data. ",
    "Please check time granularity setting."), stop = TRUE)
}

PrintPlugin("Data preparation stage completed, saving prepared data to output dataset.")

WriteDatasetWithPartitioningColumn(dfOutput, OUTPUT_DATASET_NAME)

PrintPlugin("All stages completed!")
