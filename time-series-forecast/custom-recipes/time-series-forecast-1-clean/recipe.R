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

df <- dkuReadDataset(inputDatasetName, columns = selectedColumns, colClasses = columnClasses) 

PrintPlugin("Preparation stage: date parsing, cleaning, aggregation, sorting")

# convert to R POSIX date format
df[[TIME_COLUMN]] <- as.POSIXct(df[[TIME_COLUMN]], format = dkuDateFormat)

# truncate all dates to the start of the period to avoid errors at the GenerateDateRange step
df[[TIME_COLUMN]] <- TruncateDate(df[[TIME_COLUMN]], GRANULARITY)

df <- df %>%

  # allows to use the same dataset to aggregate at higher granularities e.g. from hour to day
  group_by_(.dots = c(TIME_COLUMN)) %>%
  summarise_all(funs(AggregateNa(., AGGREGATION))) %>%

  # sort by date to avoid errors at the GenerateDateRange step
  arrange_(.dots = c(TIME_COLUMN)) %T>%

  {PrintPlugin("Resampling stage: generating a continuous date range")} %>%

  # resample the original data to a continuous date range at the chosen granularity
  GenerateDateRange(TIME_COLUMN, GRANULARITY) %T>%

  {PrintPlugin("Interpolation stage: finding and replacing outlier and or missing values")} %>%   

  # replace outlier and or missing values WARNING: heavy computational load
  CleanDF(TIME_COLUMN, SERIES_COLUMNS, GRANULARITY, 
    MISSING_VALUES, MISSING_IMPUTE_WITH, MISSING_IMPUTE_CONSTANT, 
    OUTLIERS, OUTLIERS_IMPUTE_WITH, OUTLIERS_IMPUTE_CONSTANT)

# converts the date from POSIX to a character following dataiku date format in ISO 8601 standard
df[[TIME_COLUMN]] <- strftime(df[[TIME_COLUMN]] , dkuDateFormat)

PrintPlugin("All stages completed!")

# Recipe outputs
WriteDatasetWithPartitioningColumn(df, outputDatasetName, 
  PARTITION_DIMENSION_NAME, checkPartitioning)