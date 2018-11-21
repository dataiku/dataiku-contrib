########## LIBRARY LOADING ##########

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "clean.R"))


########## INPUT OUTPUT CONFIGURATION ##########

input_dataset_name = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)
for(n in names(config)){
    assign(n, config[[n]])
}
     
# Check that partitioning settings are correct if activated
check_partition <- check_partitioning_settings(input_dataset_name,
    PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)

selected_columns <- c(TIME_COLUMN, SERIES_COLUMNS)
column_classes <- c("character", rep("numeric", length(SERIES_COLUMNS)))


########## DATA PREPARATION STAGE ##########

df <- dkuReadDataset(input_dataset_name, columns = selected_columns, colClasses = column_classes) 

plugin_print("Preparation stage: date parsing, cleaning, aggregation, sorting")

# convert to R POSIX date format
df[[TIME_COLUMN]] <- as.POSIXct(df[[TIME_COLUMN]], format = dku_date_format)

# truncate all dates to the start of the period to avoid errors at the date_range_generate step
df[[TIME_COLUMN]] <- truncate_date(df[[TIME_COLUMN]], GRANULARITY)

df <- df %>%

    # allows to use the same dataset to aggregate at higher granularities e.g. from hour to day
    group_by_(.dots = c(TIME_COLUMN)) %>%
    summarise_all(funs(aggregate_na(., AGGREGATION))) %>%

    # sort by date to avoid errors at the date_range_generate step
    arrange_(.dots = c(TIME_COLUMN)) %T>%

    {plugin_print("Resampling stage: generating a continuous date range")} %>%

    # resample the original data to a continuous date range at the chosen granularity
    date_range_generate(TIME_COLUMN, GRANULARITY) %T>%

    {plugin_print("Interpolation stage: finding and replacing outlier and or missing values")} %>%   

    # replace outlier and or missing values WARNING: heavy computational load
    clean_df(TIME_COLUMN, SERIES_COLUMNS, GRANULARITY, 
        MISSING_VALUES, MISSING_IMPUTE_WITH, MISSING_IMPUTE_CONSTANT, 
        OUTLIERS, OUTLIERS_IMPUTE_WITH, OUTLIERS_IMPUTE_CONSTANT)

# converts the date from POSIX to a character following dataiku date format in ISO 8601 standard
df[[TIME_COLUMN]] <- strftime(df[[TIME_COLUMN]] , dku_date_format)

plugin_print("All stages completed!")

# Recipe outputs
write_dataset_with_partitioning_column(df, output_dataset_name, 
    PARTITION_DIMENSION_NAME, check_partitioning)