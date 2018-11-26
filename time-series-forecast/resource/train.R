# Functions used for the Train and Evaluate recipe

library(forecast)
library(prophet)

source(file.path(dkuCustomRecipeResource(), "io.R"))

AVAILABLE_MODEL_NAME_LIST <- c(
  "NAIVE_MODEL",
  "SEASONALTREND_MODEL",
  "PROPHET_MODEL",
  "ARIMA_MODEL",
  "EXPONENTIALSMOOTHING_MODEL",
  "NEURALNETWORK_MODEL",
  "STATESPACE_MODEL"
)

MODEL_FUNCTION_NAME_LIST <- list(
  "NAIVE_MODEL" = list(modelFunction = "NaiveModelWrapper"),
  "SEASONALTREND_MODEL" = list(modelFunction = "stlf"),
  "PROPHET_MODEL" = list(modelFunction = "ProphetModelWrapper"),
  "ARIMA_MODEL" = list(modelFunction = "auto.arima"),
  "EXPONENTIALSMOOTHING_MODEL" = list(modelFunction = "ets"), 
  "NEURALNETWORK_MODEL" = list(modelFunction = "nnetar"),
  "STATESPACE_MODEL" = list(modelFunction = "tbats")
)

NaiveModelWrapper <- function(y, method = "simple", h = 10, level = c(80,95), model = NULL) {
  # Wraps naive models implementations in the forecast package in a single standard function.
  #
  # Args:
  #   y: input time series of R ts or msts class.
  #   strategy: character string describing which naive model implementation to use
  #             (one of "simple", "seasonal", "drift").
  #   h: horizon of the forecast steps.
  #   level: confidence intervals in percentage.
  #   model: added for compatibility with others model types but unused in practice
  #          since naive models do not retain a fitted state.
  #
  # Returns:
  #   Naive model
  
  model <- switch(method,
    simple = forecast::naive(y, h = h, level = level),
    seasonal = forecast::snaive(y, h = h, level = level),
    drift = forecast::rwf(y, drift = TRUE, h = h, level = level)
  )
  return(model)
}


ProphetModelWrapper <- function(df, growth = "linear", model = NULL, y = NULL, ...) {
  # Wraps Facebook Prophet model in a single standard function.
  #
  # Args:
  #   df: input data frame following the Prophet format 
  #       ("ds" column for time, "y" for series).
  #   growth: character string describing which growth model to use
  #           (one of "linear", "logistic").
  #   model: added for compatibility with others model types in order to
  #          refit an existing model without re-estimating its parameters.
  #   y: added for compatibility with others model types but unused
  #      since prophet uses the df argument as data input.
  #
  # Returns:
  #   Fitted Prophet model

  if (is.null(model)) {
    m <- prophet(df, growth = growth, ...)
  } else {
    cutoff <- max(df$ds)
    m2 <- prophet:::prophet_copy(model, cutoff)
    m <- fit.prophet(m2, df)
  }
   return(m)
}

TrainForecastingModels <- function(ts, df, modelParameterList, 
  refit = FALSE, refitModelList = NULL, verbose = TRUE) {
  # Trains or retrains multiple forecasting models on a time series according to
  # a list of model parameters and optionally previously fitted models.
  #
  # Args:
  #   ts: input time series of R ts or msts class.
  #   df: input data frame following the Prophet format 
  #       ("ds" column for time, "y" for series).
  #   modelParameterList: named list of model parameters set in the plugin UI
  #   refit: boolean, if TRUE then refit existing models without re-estimating its parameters.
  #   refitModelList: Named list of fitted models (output of a previous call to this function).
  #   verbose: boolean, if TRUE then prints details about each model training.
  #
  # Returns:
  #   Named list of fitted model

  modelList <- list()
  for(modelName in names(modelParameterList)) {
    modelParameters <- modelParameterList[[modelName]]
    if (refit && !is.null(refitModelList)) { 
      modelParameters[["kwargs"]][["model"]] <- refitModelList[[modelName]]
    }
    if (modelName == "PROPHET_MODEL") {
      modelParameters[["kwargs"]][["df"]] <- df
    }

    PrintPlugin(paste0(modelName," training starting"), verbose)
    startTime = Sys.time()
    modelList[[modelName]] <- R.utils::doCall(
      .fcn = modelParameters[["modelFunction"]],
      y = ts,
      args = modelParameters[["kwargs"]],
      .ignoreUnusedArgs = TRUE
    )
    endTime = Sys.time()
    PrintPlugin(paste0(modelName," training completed after ", 
              round(endTime - startTime, 1), " seconds"), verbose)
  }
  return(modelList)
}