library(dataiku)
R_lib_path <- paste(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastUtils.R", sep="/")
source(R_lib_path)

input_dataset_name = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
TIME_COLUMN <- config[["TIME_COLUMN"]]
SERIES_COLUMN <- config[["SERIES_COLUMN"]]
CHOSEN_GRANULARITY <- config[["CHOSEN_GRANULARITY"]]
TIMEZONE <- config[["TIMEZONE"]]
REPLACE_OUTLIER <- config[["REPLACE_OUTLIER"]]
REPLACE_MISSING <- config[["REPLACE_MISSING"]]

# TODO: (next versions) extend this to multivariate time series?

df <- dkuReadDataset(input_dataset_name,
                     columns = c(TIME_COLUMN, SERIES_COLUMN),
                     colClasses = c("character","numeric")) %T>%

        {plugin_print("Preparation stage: date parsing, cleaning, aggregation, sorting")} %>%

        # rename columns to simplify internal handling
        rename("time_column" := !!TIME_COLUMN, "series_column" := !!SERIES_COLUMN) %>%

        # convert to R POSIX date format
        mutate_at(c("time_column"), funs(as.POSIXct(., TIMEZONE, format=dku_date_format))) %>%

        # truncate all dates to the start of the period to avoid errors at the date_range_generate step
        mutate_at(c("time_column"), funs(trunc_to_granularity_start(., CHOSEN_GRANULARITY))) %>%
        
        # allows to use the same dataset to aggregate at higher granularities e.g. from hour to day
        group_by(time_column) %>%
        summarise(series_column = sum_na(series_column)) %>%

        # sort by date to avoid errors at the date_range_generate step
        arrange(time_column) %T>%

        {plugin_print("Resampling stage: generating a continuous date range")} %>%

        # resample the original data to a continuous date range at the chosen granularity
        date_range_generate(CHOSEN_GRANULARITY) %T>%

        {plugin_print("Interpolation stage: finding and replacing outlier and or missing values")} %>%

        # replace outlier and or missing values using seasonality decomposition
        # WARNING: heavy computational load
        replace_outlier_or_missing(CHOSEN_GRANULARITY, REPLACE_OUTLIER, REPLACE_MISSING) %>%

        # converts the date from POSIX to a character following dataiku date format in ISO 8601 standard
        mutate_at(c("time_column"), funs(strftime(. , dku_date_format, TIMEZONE))) %>%

        # renames the columns back to the original names in the input dataset
        rename(!!TIME_COLUMN := "time_column", !!SERIES_COLUMN := "series_column")

plugin_print("All stages completed!")

# Recipe outputs
dkuWriteDataset(df, output_dataset_name)