# Functions used for the Clean recipe

library(zoo)
library(timeDate)
library(dplyr)
library(magrittr)
library(forecast)
library(lubridate)
library(dataiku)
source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))


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


date_range_generate <- function(df, time_column, granularity) {
    #' Resample a univariate time series data.frame to a continuous date range at the chosen granularity
    #'
    #' @description First it generates the continuous date range.
    #' Then it joins the original time series back to the date range.
    #'
    #' @details The expected structure for the input time series data.frame is to have two columns:
    #' "time_column" of date or POSIX type 
    #' "series_column" of numeric type
    
    min_date <- min(df[[time_column]])
    max_date <- max(df[[time_column]])
    all_times <- tibble(!!time_column := seq(min_date, max_date, by = granularity))
    df_all_times <- merge(all_times, df, by = time_column, all.x = TRUE)
    return(df_all_times)
}

aggregation_na <- function(x, strategy){
    #' Aggregation function robust to missing values
    #'
    #' @description This returns a missing value if all input values are missing.
    #' Otherwise, it returns the aggregate of the non-missing elements.
    
    agg <- case_when(
            strategy == 'mean' ~ ifelse(all(is.na(x)), NA, mean(x, na.rm=TRUE)),
            strategy == 'sum' ~  ifelse(all(is.na(x)), NA, sum(x, na.rm=TRUE))
    )   
    return(agg)
}


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


timeseries_clean <- function(msts, missing_values, missing_impute_with, missing_impute_constant, 
                             outliers, outliers_impute_with, outliers_impute_constant) {
    if (missing_values == 'interpolate') {
        msts <- na.interp(msts)
    } 
    else if (missing_values == 'impute') {
        missing_imputation <- case_when(
            missing_impute_with == 'median' ~ median(msts, na.rm=TRUE),
            missing_impute_with == 'average' ~ mean(msts, na.rm=TRUE),
            missing_impute_with == 'constant' ~ missing_impute_constant
        )   
        msts[which(is.na(msts))] <- missing_imputation
    }
    
    outliers_detected <- tsoutliers(msts)
    if(outliers == 'interpolate'){
        msts[outliers_detected$index] <- outliers_detected$replacements
    }
    else if (outliers == 'impute') {
        outliers_imputation <- case_when(
            outliers_impute_with == 'median' ~ median(msts, na.rm=TRUE),
            outliers_impute_with == 'average' ~ mean(msts, na.rm=TRUE),
            outliers_impute_with == 'constant' ~ outliers_impute_constant
        )   
        msts[outliers_detected$index] <- outliers_imputation
    }
    return(msts)
}

df_clean <- function(df, time_column, series_columns, granularity, 
                     missing_values, missing_impute_with, missing_impute_constant, 
                     outliers, outliers_impute_with, outliers_impute_constant) {
    #' Replace missing values and/or missing values in a multivariate time series data.frame
    #'
    #' @description First it converts the input dataframe to forecast::msts multi-seasonal time series format. 
    #' Then it selectively applies the missing value interpolation and outlier replacement methods from the R forecast package.
    
    df_output <- tibble(!!time_column := df[[time_column]])
    for(c in series_columns){
        msts <- msts_conversion(df, time_column, c, granularity)
        df_output[[c]] <- as.numeric(
            timeseries_clean(
                msts, 
                missing_values, missing_impute_with, missing_impute_constant, 
                outliers, outliers_impute_with, outliers_impute_constant
            )
        )
    }   
    return(df_output)
}