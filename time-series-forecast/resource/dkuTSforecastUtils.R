library(zoo)
library(timeDate)
library(dplyr)
library(magrittr)
library(forecast)
library(lubridate)
library(R.utils)

# This is the character date format used by Dataiku DSS for dates as per the ISO8601 standard
dku_date_format = "%Y-%m-%dT%T.000Z"

plugin_print <- function(message){
    print(paste("[PLUGIN_LOG]", message))
}

trunc_to_granularity_start <- function(date, granularity) {
    #' Truncate a date to the start of the chosen granularity
    #'
    #' @description This function guarantees that date_range_generate works on all granularities.
    #' This is because the periods of month and quarters may vary across time.
    
    tmp_date <- as.POSIXlt(date)
    output_date <-  switch(granularity,
        year = as.Date(tmp_date) - tmp_date$yday,
        quarter = as.Date(zoo::as.yearqtr(tmp_date)),
        month = as.Date(tmp_date) - tmp_date$mday + 1,
        week = as.Date(tmp_date) - tmp_date$wday + 1,
        day = as.Date(tmp_date),
        hour = as.POSIXct(trunc(tmp_date, "hour")))
    return(output_date)
}

date_range_generate <- function(df, granularity) {
    #' Resample a univariate time series data.frame to a continuous date range at the chosen granularity
    #'
    #' @description First it generates the continuous date range.
    #' Then it joins the original time series back to the date range.
    #'
    #' @details The expected structure for the input time series data.frame is to have two columns:
    #' "time_column" of date or POSIX type 
    #' "series_column" of numeric type
    
    min_date <- min(df$time_column)
    max_date <- max(df$time_column)
    all_times <- data.frame(time_column = seq(min_date, max_date, by=granularity))
    df_all_times <- merge(all_times, df, by='time_column', all.x=TRUE)
    return(df_all_times)
}

sum_na <- function(x){
    #' Sum function robust to missing values
    #'
    #' @description This return a missing value if all input values are missing.
    #' Otherwise, it returns the sum of the non-missing elements.
    
    return(ifelse(all(is.na(x)),NA,sum(x, na.rm=TRUE)))
}

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

msts_conversion <- function(df, granularity) {
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
    
    min_date <- min(df$time_column)
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
            data = df[["series_column"]],
            seasonal.periods = seasonal.periods,
            start = start_date
        )
    return(output_msts)
}

replace_outlier_or_missing <- function(df, granularity, replace_outlier, replace_missing) {
    #' Replace missing values and/or missing values in a univariate time series data.frame
    #'
    #' @description First it converts the input dataframe to forecast::msts multi-seasonal time series format. 
    #' Then it selectively applies the missing value interpolation and outlier replacement methods from the R forecast package.
    #'
    #' @details The expected structure for the input time series data.frame is to have two columns:
    #' "time_column" of date or POSIX type 
    #' "series_column" of numeric type
    
    if(replace_outlier || replace_missing) {
        msts <- msts_conversion(df, granularity)
        if(replace_outlier){
            msts_clean <- msts %>% 
                forecast::tsclean(replace.missing=replace_missing)
        }
        else{
            msts_clean <- msts %>% 
                forecast::na.interp()
        }
        df_output <- msts_clean %>%
                        data.frame(time_column = df$time_column, series_column = as.numeric(.)) %>%
                        select(time_column, series_column)
                        
    }
    else{
            df_output <- df
        }
    return(df_output)
}

clean_list_mixed_type <- function(named_list){
    cleaned_list <- list()
    for(name in names(named_list)){
        if(!is.na(as.numeric(named_list[[name]]))){
            cleaned_list[[name]] <- as.numeric(named_list[[name]])
        }
        else{
            if(!is.na(as.logical(named_list[[name]]))){
                cleaned_list[[name]] <- as.logical(named_list[[name]])
            }
            else{
                cleaned_list[[name]] <- as.character(named_list[[name]])
            }
            
        }
    }
    return(cleaned_list)
}

clean_kwargs_from_param <- function(kwargs_param) {
    clean_kwargs <- clean_list_mixed_type(as.list(kwargs_param)) # handles map parameter raw format
    if(length(clean_kwargs) != 0){
        plugin_print("Additional parameters are below")
        print(clean_kwargs)
    }
    return(clean_kwargs)
}

naive_model_train <- function(ts, method, lambda, biasadj) {
    #' Wrap naive models from the forecast package in a simpler standard way
    #'
    #' @description Depending on a simple "method" argument, this wrapper function switches to 
    #' different implementations of naive models in the forecast package.
    
    plugin_print("Naive model training started")   
    
    model <- switch(method,
        simple = forecast::naive(ts, lambda = lambda, biasadj = biasadj),
        seasonal = forecast::snaive(ts, lambda = lambda, biasadj = biasadj),
        drift = forecast::rwf(ts, drift=TRUE, lambda = lambda, biasadj = biasadj)
    )
    
    plugin_print("Naive model training completed")
    
    return(model)
}


seasonaltrend_model_train <- function(ts, error_type, trend_type, seasonality_type, lambda, biasadj, kwargs) {
    #' Wrap seasonal trend models from the forecast package in a simpler standard way

    plugin_print("Seasonal trend model training started") 
    
    model_type <- paste0(error_type, trend_type, seasonality_type)
    model <- doCall("forecast", 
        object = ts,
        model = model_type,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("Seasonal trend model training completed")
    
    return(model)
}


neuralnetwork_model_train <- function(ts, non_seasonal_lags, seasonal_lags, size, lambda, biasadj, kwargs) {
    #' Wrap seasonal trend models from the forecast package in a simpler standard way

    if(non_seasonal_lags != -1){
        # -1 is for automatic selection in which case the parameter should not be assigned
        kwargs[["p"]] <- non_seasonal_lags
    }
    if(size != -1){
        # -1 is for automatic selection in which case the parameter should not be assigned
        kwargs[["size"]] <- size
    }
    
    plugin_print("Neural network model training started")
    
    # could also be used with external regressors
    model <- doCall("nnetar", 
        y = ts,
        P = seasonal_lags,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("Neural network model training completed")
    
    return(model)
}


arima_model_train <- function(ts, stepwise, lambda, biasadj, kwargs) {
    #' Wrap auto.arima models from the forecast package in a simpler standard way

    plugin_print("ARIMA model training started") 
    
    # could also be used with external regressors
    model <- doCall("auto.arima", 
        y = ts,
        stepwise = stepwise,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        parallel = TRUE,
        trace = TRUE,
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("ARIMA model training completed")
    
    return(model)
}

statespace_model_train <- function(ts, lambda, biasadj, kwargs) {
    #' Wrap tbats models from the forecast package in a simpler standard way

    plugin_print("State model training started") 
    
    # could also be used with external regressors
    model <- doCall("tbats", 
        y = ts,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("State model model training completed")
    
    return(model)
}

save_to_managed_folder <- function(folder_id, model_list, ts, ...) {
    #' Save R models and arbitrary objects to a filesystem folder in Rdata format with a defined structure
    #'
    #' @description First, it creates a structure inside the directory:
    #' - version/<timestamp in ms>/ for parameters and time series,
    #' - version/<timestamp in ms>/models for models.
    #' Then it saves the objects in Rdata format to the relevant directory.
    #' It handles versioning of models so all trained models are saved.
    
    folder_path <- dkuManagedFolderPath(folder_id)
    folder_type <- tolower(dkuManagedFolderInfo(folder_id)[["info"]][["type"]])
    if(folder_type!="filesystem") {
        stop("Output folder must be on the Server Filesystem. \
          Please use the \"filesystem_folders\" connection.")
    }
    
    # create standard directory structure
    timestamp_ms <- as.character(round(as.numeric(Sys.time())*1000))
    version_path <- file.path(folder_path, "versions", timestamp_ms)
    models_path <- file.path(version_path, "models")
    dir.create(models_path, recursive = TRUE)
    
    save(ts, file = file.path(version_path , "ts.RData"))
    save(..., file = file.path(version_path , "params.RData"))
    
    for(model_name in names(model_list)) {
        model <- model_list[[model_name]]
        if(!is.null(model)) {
            save(model, file = file.path(models_path, paste0(model_name,".RData")))
        }
    }
    plugin_print("Models, time series and parameters saved to folder")
}

load_from_managed_folder <- function(folder_id){
    input_folder_path <- dkuManagedFolderPath(folder_id)
    input_folder_type <- tolower(dkuManagedFolderInfo(folder_id)[["info"]][["type"]])
    if(input_folder_type!="filesystem") {
         stop("Input folder must be on the Server Filesystem. \
         Please use the \"filesystem_folders\" connection.")
    }
    last_version_timestamp <- max(list.files(file.path(input_folder_path, "versions")))
    version_path <- file.path(input_folder_path, "versions", last_version_timestamp)
    models_path <- file.path(version_path, "models")
    rdata_path_list <- list.files(
        path = version_path,
        pattern = "*.RData",
        full.names = TRUE,
        include.dirs = FALSE,
        recursive = TRUE
    )
    for(rdata_path in rdata_path_list){
        load(rdata_path, envir = .GlobalEnv)
    }
    plugin_print("Models, time series and parameters loaded from folder")
}