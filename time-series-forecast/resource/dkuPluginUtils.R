# Various utility functions used across the plugin for non-specific time series stuff

library(dataiku)

plugin_print <- function(message){
    print(paste("[PLUGIN_LOG]", message))
}


# This is the character date format used by Dataiku DSS for dates as per the ISO8601 standard
dku_date_format = "%Y-%m-%dT%T.000Z"

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
            }
            else{
                if(!is.na(as.logical(param[[name]]))){
                    cleaned_list[[name]] <- as.logical(param[[name]])
                }
                else{
                    cleaned_list[[name]] <- as.character(param[[name]])
                }

            }
        }
        return(cleaned_list)
    }
    else{
        return(param)
    }
}

dkuManagedFolderPathWithPartitioning <- function(folder_name, partition_dimension_name) {
    if(dkuManagedFolderDirectoryBasedPartitioning(folder_name)){
        file_path <- file.path(
            dkuManagedFolderPath(folder_name),
            dkuManagedFolderPartitionFolder(folder_name, 
                partition = dkuFlowVariable(paste0("DKU_DST_", partition_dimension_name)))
        )
        file_path <- gsub("//","/",file_path)
    } else {
        file_path <- dkuManagedFolderPath(folder_name)
    }
    return(normalizePath(file_path))
}