# Functions used for the train and evaluate recipe

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
  # Wrap naive models from the forecast package in a simpler standard way
  #
  # @description Depending on a simple "method" argument, this wrapper function switches to 
  # different implementations of naive models in the forecast package. 
  
  model <- switch(method,
    simple = forecast::naive(y, h = h, level = level),
    seasonal = forecast::snaive(y, h = h, level = level),
    drift = forecast::rwf(y, drift = TRUE, h = h, level = level)
  )
  return(model)
}


ProphetModelWrapper <- function(df, growth = "linear", model = NULL, y = NULL, ...) {
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