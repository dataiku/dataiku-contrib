library(forecast)
#library(prophet)

source(file.path(dkuCustomRecipeResource(), "clean.R"))
source(file.path(dkuCustomRecipeResource(), "train.R"))

GetForecasts <- function(ts, df, xreg = NULL, modelList, modelParameterList,
  horizon, granularity, confidenceInterval = 95, includeHistory = FALSE) {
  # Gets forecast values from forecast models
  #
  # Args:
  #   ts: input time series of R ts or msts class.
  #   df: input data frame following the Prophet format
  #       ("ds" column for time, "y" for series).
  #   xreg: matrix of external regressors (optional)
  #   modelList: named list of models (output of a call to the TrainForecastingModels function).
  #   modelParameterList: named list of model parameters set in the "Train and Evaluate" recipe UI.
  #   horizon: horizon of the forecast steps.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #   confidenceInterval: confidence interval in percentage.
  #   includeHistory: boolean, if TRUE then include historical one-step forecasts
  #
  # Returns:
  #   Data.frame with forecast values and confidence intervals

  if (!is.null(xreg) && nrow(xreg) != 0) {
    horizon <- nrow(xreg)
  }
  # generate date range for history and/or future
  if (includeHistory) {
    dateRange <- seq(min(df$ds), by = granularity, length = nrow(df) + horizon)
  } else {
    dateRange <- tail(seq(max(df$ds), by = granularity, length = horizon + 1), -1)
  }
  dateRange <- TruncateDate(dateRange, granularity)
  forecastDfList <- list()
  for(modelName in names(modelList)) {
    model <- modelList[[modelName]]
    # if (modelName == "PROPHET_MODEL") {
    #   freq <- ifelse(granularity == "hour", 3600, granularity)
    #   future <- make_future_dataframe(model, horizon, freq, include_history = includeHistory)
    #   if (!is.null(xreg)) {
    #     for (c in colnames(xreg)) {
    #       if (includeHistory) {
    #         future[,c] <- c(df[,c], xreg[,c])
    #       } else {
    #         future[,c] <- xreg[,c]
    #       }
    #     }
    #   }
    #   model$interval.width <- confidenceInterval / 100.0
    #   forecastDf <- stats::predict(model, future) %>%
    #     select_(.dots = c("ds", "yhat", "yhat_lower", "yhat_upper"))
    #   forecastDf$ds <- dateRange # harmonizes dates with other model types
    # } else {
      # special cases for naive and seasonal trend model which cannot use forecast(model, h)
      # they can only be called directly with a horizon argument
      if (modelName %in% c("NAIVE_MODEL","SEASONALTREND_MODEL")) {
        f <- R.utils::doCall(
            .fcn = modelParameterList[[modelName]][["modelFunction"]],
            y = ts,
            h = horizon,
            level = c(confidenceInterval),
            args = modelParameterList[[modelName]][["kwargs"]],
            .ignoreUnusedArgs = FALSE
        )
      } else if (modelName == "NEURALNETWORK_MODEL") {
        # neural networks in forecast need a special PI argument to get confidence intervals
        if (!is.null(xreg)) {
          f <- forecast(model, xreg = xreg, level = c(confidenceInterval), PI = TRUE)
        } else {
          f <- forecast(model, h = horizon, level = c(confidenceInterval), PI = TRUE)
        }
      } else if (modelName == "ARIMA_MODEL") {
        if (!is.null(xreg)) {
          f <- forecast(model, xreg = xreg, level = c(confidenceInterval))
        } else {
          f <- forecast(model, h = horizon, level = c(confidenceInterval))
        }
      } else {
        # general case for other model types
        model$h <- horizon
        f <- forecast(ts, model = model, h = horizon, level = c(confidenceInterval))
      }
      if (includeHistory) {
        forecastDf <- tibble(
          ds = dateRange,
          yhat = c(f$fitted, as.numeric(f$mean)[1:horizon]),
          yhat_lower = c(rep(NA, nrow(df)), as.numeric(f$lower)[1:horizon]),
          yhat_upper = c(rep(NA, nrow(df)), as.numeric(f$upper)[1:horizon]),
        )
      } else {
        forecastDf <- tibble(
          ds = dateRange,
          yhat = as.numeric(f$mean)[1:horizon],
          yhat_lower = as.numeric(f$lower)[1:horizon],
          yhat_upper = as.numeric(f$upper)[1:horizon],
        )
      }
     # }
    forecastDfList[[modelName]] <- forecastDf
  }
  return(forecastDfList)
}


CombineForecastHistory <- function(historyDf = NULL, forecastDf = NULL,
  includeForecast = TRUE, includeHistory = FALSE) {
  # Combines historical and forecast data.frames
  #
  # Args:
  #   historyDf: data.frame of historical values following the Prophet format.
  #   forecastDf: data.frame of forecasts (output of a call to the GetForecasts function).
  #   includeForecast: boolean, if TRUE then include future forecasts.
  #   includeHistory: boolean, if TRUE then include historical values, one-step forecasts & residuals.
  #
  # Returns:
  #   Data.frame with historical values and/or forecast values and residuals

  if (includeForecast && includeHistory) {
    dfOutput <- merge(historyDf, forecastDf, by = "ds", all = TRUE)
    dfOutput["residuals"] <- dfOutput["y"] - dfOutput["yhat"]
    dfOutput["origin"] <- ifelse(is.na(dfOutput[["y"]]), "forecast","history")
  } else if (includeForecast && !includeHistory) {
    dfOutput <- forecastDf
    dfOutput["origin"] <- "forecast"
  } else if (!includeForecast && includeHistory) {
    dfOutput <- merge(historyDf, forecastDf, by = "ds", all.y = FALSE) %>%
      select_(.dots = c("ds", "y", "yhat"))
    dfOutput["residuals"] <- dfOutput["y"] - dfOutput["yhat"]
    dfOutput["origin"] <- "history"
  }
  return(dfOutput)
}
