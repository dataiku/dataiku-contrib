# Functions used for the Clean recipe

library(zoo)
library(timeDate)
library(dplyr)
library(tibble)
library(magrittr)
library(forecast)
library(lubridate)

source(file.path(dkuCustomRecipeResource(), "io.R"))

# This is the character date format used by Dataiku DSS for dates as per the ISO8601 standard
dkuDateFormat = "%Y-%m-%dT%T.000Z"
op <- options(digits.secs = 3)

# This fixed mapping is used to convert time series from R data.frame to forecast::msts format
# This is needed as the forecast::msts format requires seasonality arguments
mapGranularitySeasonality <- list(
  year = c(1),  
  quarter = c(4),
  month = c(12),
  week = c(365.25/7),
  day = c(365.25, 7), # seasonality within year, week
  hour = c(8766, 168, 24) # seasonality within year, week, day
)

GenerateDateRange <- function(df, timeColumn, granularity) {
  #' Resample a univariate time series data.frame to a continuous date range at the chosen granularity
  #'
  #' @description First it generates the continuous date range.
  #' Then it joins the original time series back to the date range.
  #'
  #' @details The expected structure for the input time series data.frame is to have two columns:
  #' "timeColumn" of date or POSIX type 
  #' "seriesColumn" of numeric type
  
  minDate <- min(df[[timeColumn]])
  maxDate <- max(df[[timeColumn]])
  allTimes <- tibble(!!timeColumn := seq(minDate, maxDate, by = granularity))
  dfAllTimes <- merge(allTimes, df, by = timeColumn, all.x = TRUE)
  return(dfAllTimes)
}

TruncateDate <- function(date, granularity) {
  #' Truncate a date to the start of the chosen granularity
  #'
  #' @description This function guarantees that GenerateDateRange works on all granularities.
  #' This is because the periods of month and quarters may vary across time.
  
  tmpDate <- as.POSIXlt(date)
  outputDate <-  switch(granularity,
    year = as.Date(tmpDate) - tmpDate$yday,
    quarter = as.Date(zoo::as.yearqtr(tmpDate)),
    month = as.Date(tmpDate) - tmpDate$mday + 1,
    week = as.Date(tmpDate) - tmpDate$wday + 1,
    day = as.Date(tmpDate),
    hour = as.POSIXct(trunc(tmpDate, "hour")))
  return(outputDate)
}

AggregateNa <- function(x, strategy) {
  #' Aggregation function robust to missing values
  #'
  #' @description This returns a missing value if all input values are missing.
  #' Otherwise, it returns the aggregate of the non-missing elements.
  
  agg <- case_when(
      strategy == 'mean' ~ ifelse(all(is.na(x)), NA, mean(x, na.rm = TRUE)),
      strategy == 'sum' ~  ifelse(all(is.na(x)), NA, sum(x, na.rm = TRUE))
  )   
  return(agg)
}

ConvertDFtoTS <- function(df, timeColumn, seriesColumn, granularity) {
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
  #' "timeColumn" of date or POSIX type 
  #' "seriesColumn" of numeric type
  
  minDate <- min(df[[timeColumn]])
  seasonal.periods <- mapGranularitySeasonality[[granularity]]
  startDate <- c() # vector of two elements according to the ts object documentation
  startDate[1] <- lubridate::year(minDate) # year
  startDate[2] <- switch(granularity,
    year = 1,
    quarter = lubridate::quarter(minDate),
    month = lubridate::month(minDate),
    week = lubridate::week(minDate),
    day = lubridate::yday(minDate),
    hour = 24 * (lubridate::yday(minDate) - 1) + lubridate::hour(minDate),
  )
  ts <- msts(
      data = df[[seriesColumn]],
      seasonal.periods = seasonal.periods,
      start = startDate
    )
  return(ts)
}

CleanTS <- function(ts, missingValues, missingImputeWith, missingImputeConstant, 
               outliers, outliersImputeWith, outliersImputeConstant) {
  if (missingValues == 'interpolate') {
    ts <- na.interp(ts)
  } else if (missingValues == 'impute') {
    missingImputation <- case_when(
      missingImputeWith == 'median' ~ median(ts, na.rm = TRUE),
      missingImputeWith == 'average' ~ mean(ts, na.rm = TRUE),
      missingImputeWith == 'constant' ~ missingImputeConstant
    )   
    msts[which(is.na(ts))] <- missingImputation
  }
  
  outliersDetected <- tsoutliers(ts)
  if (outliers == 'interpolate') {
    ts[outliersDetected$index] <- outliersDetected$replacements
  } else if (outliers == 'impute') {
    outliersImputation <- case_when(
      outliersImputeWith == 'median' ~ median(ts, na.rm = TRUE),
      outliersImputeWith == 'average' ~ mean(ts, na.rm = TRUE),
      outliersImputeWith == 'constant' ~ outliersImputeConstant
    )   
    ts[outliersDetected$index] <- outliersImputation
  }
  return(ts)
}

CleanDF <- function(df, timeColumn, seriesColumns, granularity, 
           missingValues, missingImputeWith, missingImputeConstant, 
           outliers, outliersImputeWith, outliersImputeConstant) {
  #' Replace missing values and/or missing values in a multivariate time series data.frame
  #'
  #' @description First it converts the input dataframe to forecast::msts multi-seasonal time series format. 
  #' Then it selectively applies the missing value interpolation and outlier replacement methods from the R forecast package.
  
  dfOutput <- tibble(!!timeColumn := df[[timeColumn]])
  for(c in seriesColumns) {
    ts <- ConvertDFtoTS(df, timeColumn, c, granularity)
    dfOutput[[c]] <- as.numeric(
      CleanTS(ts, missingValues, missingImputeWith, missingImputeConstant, 
        outliers, outliersImputeWith, outliersImputeConstant
      )
    )
  }   
  return(dfOutput)
}