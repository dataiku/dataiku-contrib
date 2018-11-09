# Various utility functions used across the plugin for non-specific time series stuff

library(dataiku)
library(jsonlite)

plugin_print <- function(message, verbose = TRUE){
    if(verbose) print(paste("[PLUGIN_LOG]", message))
}

# This is the character date format used by Dataiku DSS for dates as per the ISO8601 standard
dku_date_format = "%Y-%m-%dT%T.000Z"
op <- options(digits.secs = 3)

# This fixed mapping is used to convert time series from R data.frame to forecast::msts format
# This is needed as the forecast::msts format requires seasonality arguments
map_granularity_seasonality <- list(
    year = c(1),  # no seasonality
    quarter = c(4), # seasonality within year
    month = c(12), # seasonality within year
    week = c(365.25/7), # seasonality within year
    day = c(365.25, 7), # seasonality within year, week
    hour = c(8766, 168, 24) # seasonality within year, week, day
)

msts_conversion <- function(df, time_column, series_column, granularity) {
    #' Converts a univariate time series from R data.frame to forecast::msts multi-seasonal time series format
    #'
    #' @description First the functions computes the start date and converts it to the numeric vector expected by the ts object.
    #' This vector requires two numeric elements: 
    #'
    #' 1. The year in yyyy format
    #'
    #' 2. The numeric fraction of the period inside the year e.g. 3/12. for the third month of the year 
    #' and 10/365.25 for the 10th day of the year
    #' 
    #' Hence, the content of this vector depends on the chosen granularity.
    #'
    #' Finally, it takes the original series data and builds it into a msts object.
    #' 
    #' @details The expected structure for the input time series data.frame is to have two columns:
    #' "time_column" of date or POSIX type 
    #' "series_column" of numeric type
    
    min_date <- min(df[[time_column]])
    seasonal.periods <- map_granularity_seasonality[[granularity]]
    start_date <- c() # vector of two elements according to the ts object documentation
    start_date[1] <- lubridate::year(min_date) # year
    start_date[2] <- switch(granularity,
        year = 1,
        quarter = lubridate::quarter(min_date),
        month = lubridate::month(min_date),
        week = lubridate::week(min_date),
        day = lubridate::yday(min_date),
        hour = 24 * (lubridate::yday(min_date) - 1) + lubridate::hour(min_date),
    )
    output_msts <- msts(
            data = df[[series_column]],
            seasonal.periods = seasonal.periods,
            start = start_date
        )
    return(output_msts)
}


clean_plugin_param <- function(param){
    #' Infers types for a named list
    #'
    #' @description This returns a list with inferred types. 
    #' First it checks for numeric type, then boolean, and finally defaults to character
    if(is.list(param)){
        cleaned_list <- list()
        for(name in names(param)){
            if(!is.na(as.numeric(param[[name]]))){
                cleaned_list[[name]] <- as.numeric(param[[name]])
            } else{
                if(!is.na(as.logical(param[[name]]))){
                    cleaned_list[[name]] <- as.logical(param[[name]])
                } else{
                    cleaned_list[[name]] <- as.character(param[[name]])
                }
            }
        }
        return(cleaned_list)
    } else {
        return(param)
    }
}

check_partitioning_setting_input_output <- function(input_dataset_name, partitioning_activated, partition_dimension_name){
    check <- 'NOK' # can be OK, NOK, or NP (not partitioned)
    error_msg <- ''

    input_is_partitioned <- dkuListDatasetPartitions(input_dataset_name)[1] !='NP' 
    if(!partitioning_activated && !input_is_partitioned){
        check <- "NP"
    } else if (!partitioning_activated && input_is_partitioned){
        error_msg <- "Partitioning should activated in the recipe settings as input is partitioned"
    } else { # check partition_dimension_name recipe settings if partitioning activated
        flow_variables_available <- Sys.getenv("DKU_CALL_ORIGIN") != 'notebook'
        if(flow_variables_available){
            flow_variables <- fromJSON(Sys.getenv("DKUFLOW_VARIABLES"))
            output_dimension_is_valid <- paste0("DKU_DST_", partition_dimension_name) %in% names(flow_variables)
            if(input_is_partitioned){ 
                if(is.null(partition_dimension_name) || partition_dimension_name == ''){
                    error_msg <- "Partitioning dimension name is required"
                } else if(!output_dimension_is_valid) {
                    error_msg <- paste0("Dimension name '", partition_dimension_name,"' is invalid or output is not partitioned")
                } else {
                    check <- "OK"
                }
            } else {
                if(!is.null(partition_dimension_name) && partition_dimension_name != '') {
                    error_msg <- "Partitioning dimension name should be left blank if input dataset is not partitioned"
                } else if(output_dimension_is_valid) {
                    error_msg <- "All input and output should be partitioned"
                } else {
                    check <- "NP"
                }
            }
        } else {
            check <- ifelse(input_is_partitioned, "OK", "NP")
        }
    }
    
    plugin_print(paste0("Partitioning check returned ", check))
    if(check=='NOK') {
        stop(paste0("[ERROR] ", error_msg))
    } else {
        return(check)
    }
}

write_dataset_with_partitioning_column <- function(df, output_dataset_name, partition_dimension_name, check_partitioning){
    output_fullName <- dataiku:::dku__resolve_smart_name(output_dataset_name) # bug with naming from plugins on DSS 5.0.2
    output_id <- dataiku:::dku__ref_to_name(output_fullName)
    output_dataset_type <- dkuGetDatasetLocationInfo(output_id)[["locationInfoType"]]
    if(check_partition == 'OK' && output_dataset_type != 'SQL') {
        plugin_print("Writing partition value as new column")
        partitioning_column_name <- paste0("_dku_partition_", partition_dimension_name)
        df[[partitioning_column_name]] <- dkuFlowVariable(paste0("DKU_DST_", partition_dimension_name))
        df <- df %>% select(partitioning_column_name, everything())
    }
    dkuWriteDataset(df, output_dataset_name)
}

get_folder_path_with_partitioning <- function(folder_name, partition_dimension_name, check_partitioning) {
    is_output_folder_partitioned <- dkuManagedFolderDirectoryBasedPartitioning(folder_name)
    if(check_partition == 'OK' && is_output_folder_partitioned){
        file_path <- file.path(
            dkuManagedFolderPath(folder_name),
            dkuManagedFolderPartitionFolder(folder_name, 
                partition = dkuFlowVariable(paste0("DKU_DST_", partition_dimension_name)))
        )
        file_path <- normalizePath(gsub("//","/",file_path))
    } else if(check_partition == 'OK' && ! is_output_folder_partitioned){
        stop("[ERROR] Partitioning should be activated on all input and output")
    } else {
        file_path <- dkuManagedFolderPath(folder_name)
    }
    return(file_path)
}