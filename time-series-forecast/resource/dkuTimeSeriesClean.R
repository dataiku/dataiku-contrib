# Functions used for the Clean recipe

library(zoo)
library(timeDate)
library(dplyr)
library(magrittr)
library(forecast)
library(lubridate)
library(dataiku)
source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))


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