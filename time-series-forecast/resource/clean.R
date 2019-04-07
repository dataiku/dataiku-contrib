library(zoo)
library(timeDate)
library(dplyr)
library(tibble)
library(magrittr)
library(forecast)
library(lubridate)

source(file.path(dkuCustomRecipeResource(), "io.R"))

# This is the character date format used by Dataiku DSS for dates as per the ISO8601 standard.
# It is needed to parse dates from Dataiku datasets.
dkuDateFormat = "%Y-%m-%dT%T.000Z"
alternativeDateFormats = c(dkuDateFormat, "%Y-%m-%dT%T.000000Z", "%Y-%m-%dT%T")

# This fixed mapping is used to convert time series from R data.frame to forecast::msts format.
# It is needed as forecast::msts format requires seasonality arguments.
mapGranularitySeasonality <- list(
  year = c(1),
  quarter = c(4),
  month = c(12),
  week = c(365.25/7),
  day = c(365.25, 7), # seasonality within year, week
  hour = c(8766, 168, 24) # seasonality within year, week, day
)

AggregateNa <- function(x, strategy) {
  # Aggregates a numeric object in a way that is robust to missing values.
  # It can be applied in a dplyr group_by pipeline.
  #
  # Args:
  #   x: numerical array or matrix
  #   strategy: character string describing how to aggregate (one of "mean", "sum").
  #
  # Returns:
  #   Sum or average of non-missing values of x (or NA if x has no values).

  agg <- case_when(
      strategy == 'mean' ~ ifelse(all(is.na(x)), NA, mean(x, na.rm = TRUE)),
      strategy == 'sum' ~  ifelse(all(is.na(x)), NA, sum(x, na.rm = TRUE))
  )
  return(agg)
}

TruncateDate <- function(date, granularity) {
  # Truncates a date to the start of the chosen granularity.
  # It guarantees that ResampleDataframeWithTimeSeries function works at yearly/quarterly/monthly granularity.
  # Indeed they have varying lengths contrary to week/day/hour granularity.
  #
  # Args:
  #   date: object of POSIX or Date class. It can be an atomic date or an array of dates.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour")
  #
  # Returns:
  #   Truncated date object. For weekly granularity, we consider that weeks start on Monday.

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

ConvertDataFrameToTimeSeries <- function(df, timeColumn, seriesColumn, granularity) {
  # Converts a univariate time series from data.frame to forecast::msts time series format.
  # It assumes that the time column is a continuous date range at chosen granularity.
  #
  # Args:
  #   df: data.frame with one time column and any number of series columns.
  #   timeColumn: name of the time column. Must be of POSIX or Date class.
  #   seriesColumn: name of the numeric column for the time series values.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #
  # Returns:
  #   Multi-seasonal time series for the chosen time and series columns.

  minDate <- min(df[[timeColumn]])
  seasonal.periods <- mapGranularitySeasonality[[granularity]]

  # Vector of two elements according to the ts object documentation:
  # 1. The year in yyyy format,
  # 2. The numeric fraction of the period inside the year
  # e.g. 10/365.25 for the 10th day of the year.
  startDate <- c()
  startDate[1] <- lubridate::year(minDate)
  startDate[2] <- switch(granularity, # ts expects a decimal within the year
    year = 1,
    quarter = lubridate::quarter(minDate),
    month = lubridate::month(minDate),
    week = lubridate::week(minDate),
    day = lubridate::yday(minDate),
    hour = 24 * (lubridate::yday(minDate) - 1) + lubridate::hour(minDate),
  )
  ts <- forecast::msts(
      data = df[[seriesColumn]],
      seasonal.periods = seasonal.periods,
      start = startDate
    )
  return(ts)
}

PrepareDataframeWithTimeSeries <- function(df, timeColumn, seriesColumns,
  granularity, aggregationStrategy = "sum", resample = TRUE) {
  # Parses and optionally resamples time series in the data.frame at chosen granularity.
  #
  # Args:
  #   df: data.frame with one time column in Dataiku parsed date format
  #       and any number of numeric series columns.
  #   timeColumn: name of the time columnn. Must be of dataiku parsed Date format
  #   seriesColumns: name of the numeric columns for the time series values.
  #   resample: boolean, if TRUE then resample else just parse and truncate date at granularity.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #   aggregationStrategy: character string (one of "mean", "sum").
  #
  # Returns:
  #   data frame with the prepared time series

  if (granularity == "hour") {
    df[[timeColumn]] <- as.POSIXct(df[[timeColumn]], tryFormats = alternativeDateFormats)
  } else {
    df[[timeColumn]] <- as.Date(df[[timeColumn]], tryFormats = alternativeDateFormats)
  }
  # Get continuous range of dates
  minDate <- TruncateDate(min(df[[timeColumn]]), granularity)
  maxDate <- TruncateDate(max(df[[timeColumn]]), granularity)
  dateRange <- tibble(!!timeColumn := seq(minDate, maxDate, by = granularity))
  if (resample) {
    PrintPlugin("Preparation stage: date parsing, cleaning, aggregation, resampling")
    df[[timeColumn]] <- TruncateDate(df[[timeColumn]], granularity)
    dfOutput <- df %>%
      group_by_(.dots = c(timeColumn)) %>%
      summarise_all(funs(AggregateNa(., aggregationStrategy))) %>%
      arrange_(.dots = c(timeColumn))
    dfOutput <- merge(dateRange, dfOutput, by = timeColumn, all.x = TRUE)
  } else {
    # even if we do not perform aggregation and resampling, we need to check
    # that dataframe is not irregularly sampled which would cause models to fail
    if (nrow(dateRange) != nrow(df)) {
      PrintPlugin(paste0("Data is not sampled at ", GRANULARITY, " granularity"), stop = TRUE)
    }
    dfOutput <- df
  }
  return(dfOutput)
}

CleanDataframeWithTimeSeries <- function(df, timeColumn, seriesColumns, granularity,
  missingValues, missingImputeWith, missingImputeConstant,
  outliers, outliersImputeWith, outliersImputeConstant) {
  # Cleans multiple time series in a data.frame from missing values and outliers.
  # It can use interpolation techniques from the forecast package,
  # or simple methods like replacing with median, average or constant.
  #
  # Args:
  #   df: data.frame with one time column and any number of numeric series columns.
  #   timeColumn: name of the time columnn. Must be of dataiku parsed Date format
  #   seriesColumns: name of the numeric columns for the time series values.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #   missingValues: character string describing how to replace missing values
  #                  (one of "impute", "interpolate" or else no processing is applied).
  #   missingImputeWith: If missingValues is "impute", character string
  #                      describing how to replace missing values
  #                      (one of "median", "average", "constant").
  #   missingImputeConstant: Constant to impute missing values.
  #   outliers: character string describing how to replace detected outliers
  #             (one of "impute", "interpolate" or else no processing is applied).
  #   outliersImputeWith: If outliers is "impute", character string
  #                      describing how to replace outliers
  #                      (one of "median", "average", "constant").
  #   outliersImputeConstant: Constant to impute outliers.
  #
  # Returns:
  #   Cleaned data frame with the time series

  PrintPlugin("Interpolation stage: finding and replacing outlier and/or missing values")
  dfOutput <- tibble(!!timeColumn := df[[timeColumn]])
  for(seriesColumn in seriesColumns) {
    ts <- ConvertDataFrameToTimeSeries(df, timeColumn, seriesColumn, granularity)
    if (missingValues == 'interpolate') {
      ts <- forecast::na.interp(ts)
    } else if (missingValues == 'impute') {
      missingImputation <- case_when(
        missingImputeWith == 'median' ~ median(ts, na.rm = TRUE),
        missingImputeWith == 'average' ~ mean(ts, na.rm = TRUE),
        missingImputeWith == 'constant' ~ missingImputeConstant
      )
      ts[which(is.na(ts))] <- missingImputation
    }
    if (outliers == 'interpolate') {
      outliersDetected <- forecast::tsoutliers(ts)
      ts[outliersDetected$index] <- outliersDetected$replacements
    } else if (outliers == 'impute') {
      outliersImputation <- case_when(
        outliersImputeWith == 'median' ~ median(ts, na.rm = TRUE),
        outliersImputeWith == 'average' ~ mean(ts, na.rm = TRUE),
        outliersImputeWith == 'constant' ~ outliersImputeConstant
      )
      ts[outliersDetected$index] <- outliersImputation
    }
    dfOutput[[seriesColumn]] <- as.numeric(ts)
  }
  # converts the date from R POSIX class back to the dataiku date string format
  dfOutput[[timeColumn]] <- strftime(dfOutput[[timeColumn]] , dkuDateFormat)
  return(dfOutput)
}
