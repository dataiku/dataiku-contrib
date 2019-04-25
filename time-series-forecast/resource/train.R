library(forecast)
#library(prophet)

source(file.path(dkuCustomRecipeResource(), "io.R"))

# Identifiers for models. Used as a naming convention for all lists storing forecasting objects.
AVAILABLE_MODEL_NAME_LIST <- c(
  "NAIVE_MODEL",
  # "PROPHET_MODEL",
  "SEASONALTREND_MODEL",
  "EXPONENTIALSMOOTHING_MODEL",
  "ARIMA_MODEL",
  "STATESPACE_MODEL",
  "NEURALNETWORK_MODEL"
)

# Maps each model name to the actual function used for model training.
MODEL_FUNCTION_NAME_LIST <- list(
  "NAIVE_MODEL" = list(modelFunction = "NaiveModelWrapper"), # wrapper around forecast package
  # "PROPHET_MODEL" = list(modelFunction = "ProphetModelWrapper"), # wrapper around prophet package
  "SEASONALTREND_MODEL" = list(modelFunction = "stlf"), # forecast package
  "EXPONENTIALSMOOTHING_MODEL" = list(modelFunction = "ets"), # forecast package
  "ARIMA_MODEL" = list(modelFunction = "auto.arima"), # forecast package
  "STATESPACE_MODEL" = list(modelFunction = "tbats"), # forecast package
  "NEURALNETWORK_MODEL" = list(modelFunction = "nnetar") # forecast package
)

# Maps each model name to the name used in the UI
MODEL_UI_NAME_LIST <- list(
  "NAIVE_MODEL" = "Baseline",
  # "PROPHET_MODEL" = "Prophet",
  "SEASONALTREND_MODEL" = "Seasonal Trend",
  "EXPONENTIALSMOOTHING_MODEL" = "Exponential Smoothing",
  "ARIMA_MODEL" = "ARIMA",
  "STATESPACE_MODEL" = "State Space",
  "NEURALNETWORK_MODEL" = "Neural Network"
)
MODEL_UI_NAME_LIST_REV <- split(names(MODEL_UI_NAME_LIST), unlist(MODEL_UI_NAME_LIST))

# List of forecast models which support external regressors
MODELS_WITH_XREG_SUPPORT = c("ARIMA_MODEL","NEURALNETWORK_MODEL")

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

# ProphetModelWrapper <- function(df, growth = "linear", model = NULL, xreg = NULL, y = NULL, ...) {
#   # Wraps Facebook Prophet model in a single standard function.
#   #
#   # Args:
#   #   df: input data frame following the Prophet format
#   #       ("ds" column for time, "y" for series).
#   #   growth: character string describing which growth model to use
#   #           (one of "linear", "logistic").
#   #   model: added for compatibility with others model types in order to
#   #          refit an existing model without re-estimating its parameters.
#   #   y: added for compatibility with others model types but unused
#   #      since prophet uses the df argument as data input.
#   #   xreg: matrix of external regressors (optional)
#   #   ...: additional arguments passed to the original prophet function
#   #
#   # Returns:
#   #   Fitted Prophet model

#   if (is.null(model)) {
#     m <- prophet(growth = growth, ...)
#     if(!is.null(xreg)) {
#       for(columnName in colnames(xreg)) {
#         m <- add_regressor(m, columnName)
#       }
#     }
#     m <- fit.prophet(m, df)
#   } else {
#     cutoff <- max(df$ds)
#     m2 <- prophet:::prophet_copy(model, cutoff)
#     m <- fit.prophet(m2, df)
#   }
#    return(m)
# }

TrainForecastingModels <- function(ts, df, xreg = NULL, modelParameterList,
  refit = FALSE, refitModelList = NULL, verbose = TRUE) {
  # Trains or retrains multiple forecast models on a time series according to
  # a list of model parameters and optionally previously fitted models.
  #
  # Args:
  #   ts: input time series of R ts or msts class.
  #   df: input data frame following the Prophet format
  #       ("ds" column for time, "y" for series).
  #   xreg: matrix of external regressors (optional)
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
    # if (modelName == "PROPHET_MODEL") {
    #   modelParameters[["kwargs"]][["df"]] <- df
    # }
    if(modelName %in% MODELS_WITH_XREG_SUPPORT) {
      modelParameters[["kwargs"]][["xreg"]] <- xreg
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
