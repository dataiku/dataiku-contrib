# Functions used for the Predict recipe

library(forecast)
library(prophet)

source(file.path(dkuCustomRecipeResource(), "clean.R"))
source(file.path(dkuCustomRecipeResource(), "train.R"))

GetForecasts <- function(ts, df, modelList, modelParameterList, 
                     horizon, granularity, confidenceInterval = 95, 
                     includeHistory = FALSE) {
  forecastDfList <- list()
  if (includeHistory) {
    dateRange <- seq(min(df$ds), by = granularity, length = nrow(df) + horizon)
  } else {
    dateRange <- tail(seq(max(df$ds), by = granularity, length = horizon + 1), -1)
  }
  dateRange <- TruncateDate(dateRange, granularity)
  for(modelName in names(modelList)) {
    model <- modelList[[modelName]]
    if (modelName == "PROPHET_MODEL") {
      freq <- ifelse(granularity == "hour", 3600, granularity)
      future <- make_future_dataframe(model, horizon, freq, include_history = includeHistory)
      model$interval.width <- confidenceInterval / 100.0
      forecastDF <- stats::predict(model, future) %>%
        select_(.dots = c("ds", "yhat", "yhat_lower", "yhat_upper"))
      forecastDF$ds <- dateRange # harmonizes dates with other model types
    } else {
      # add special cases for naive and seasonal trend model which cannot use forecast(model, h) 
      # they can only be called directly with a horizon argument
      # forecast is not very consistent in its way of working :/ not all models can be fitted
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
        f <- forecast(model, h = horizon, level = c(confidenceInterval), PI = TRUE)
      } else {
        model$h <- horizon
        f <- forecast(ts, model = model, h = horizon, level = c(confidenceInterval))
      }
      if (includeHistory) {
        forecastDF <- tibble(
          ds = dateRange,
          yhat = c(f$fitted, as.numeric(f$mean)[1:horizon]),
          yhat_lower = c(rep(NA, nrow(df)), as.numeric(f$lower)[1:horizon]),
          yhat_upper = c(rep(NA, nrow(df)), as.numeric(f$upper)[1:horizon]),
        )
      } else {
        forecastDF <- tibble(
          ds = dateRange,
          yhat = as.numeric(f$mean)[1:horizon],
          yhat_lower = as.numeric(f$lower)[1:horizon],
          yhat_upper = as.numeric(f$upper)[1:horizon],
        )
      }
     }
    forecastDfList[[modelName]] <- forecastDF
  }
  return(forecastDfList)
}


CombineForecastHistory <- function(historyDF = NULL, forecastDF = NULL, 
                   includeForecast = TRUE, includeHistory = FALSE) {
  if (includeForecast && includeHistory) {
    dfOutput <- merge(historyDF, forecastDF, by = "ds", all = TRUE)
    dfOutput["residuals"] <- dfOutput["y"] - dfOutput["yhat"]
    dfOutput["origin"] <- ifelse(is.na(dfOutput[["y"]]), "forecast","history")
  } else if (includeForecast && !includeHistory) {
    dfOutput <- forecastDF
    dfOutput["origin"] <- "forecast"
  } else if (!includeForecast && includeHistory) {
    dfOutput <- merge(historyDF, forecastDF, by = "ds", all.y = FALSE) %>% 
      select_(.dots = c("ds", "y", "yhat"))
    dfOutput["residuals"] <- dfOutput["y"] - dfOutput["yhat"]
    dfOutput["origin"] <- "history"
  }
  return(dfOutput)
}