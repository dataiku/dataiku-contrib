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
    "NAIVE_MODEL" = list(model_function = "naive_model_wrapper"),
    "SEASONALTREND_MODEL" = list(model_function = "stlf"),
    "PROPHET_MODEL" = list(model_function = "prophet_model_wrapper"),
    "ARIMA_MODEL" = list(model_function = "auto.arima"),
    "EXPONENTIALSMOOTHING_MODEL" = list(model_function = "ets"), 
    "NEURALNETWORK_MODEL" = list(model_function = "nnetar"),
    "STATESPACE_MODEL" = list(model_function = "tbats")
)

naive_model_wrapper <- function(y, method="simple", h=10, level=c(80,95), model=NULL) {
    #' Wrap naive models from the forecast package in a simpler standard way
    #'
    #' @description Depending on a simple "method" argument, this wrapper function switches to 
    #' different implementations of naive models in the forecast package. 
    
    model <- switch(method,
        simple = forecast::naive(y, h=h, level=level),
        seasonal = forecast::snaive(y, h=h, level=level),
        drift = forecast::rwf(y, drift=TRUE, h=h, level=level)
    )
    return(model)
}


prophet_model_wrapper <- function(df, growth='linear', model=NULL, y=NULL, ...){
    if(is.null(model)){
        m <- prophet(df, growth=growth, ...)
    }
    else{
        cutoff <- max(df$ds)
        m2 <- prophet:::prophet_copy(model, cutoff)
        m <- fit.prophet(m2, df)
    }
   return(m)
}

train_forecasting_models <- function(ts, df, model_parameter_list, 
                                    refit=FALSE, refit_model_list=NULL, verbose=TRUE) {
    model_list <- list()
    for(model_name in names(model_parameter_list)){
        model_parameters <- model_parameter_list[[model_name]]
        if(refit && !is.null(refit_model_list)){
            model_parameters[["kwargs"]][["model"]] <- refit_model_list[[model_name]]
        }
        if(model_name == "PROPHET_MODEL"){
            model_parameters[["kwargs"]][["df"]] <- df
        }

        plugin_print(paste0(model_name," training starting"), verbose)
        start_time = Sys.time()
        model_list[[model_name]] <- R.utils::doCall(
            .fcn = model_parameters[["model_function"]],
            y = ts,
            args = model_parameters[["kwargs"]],
            .ignoreUnusedArgs = TRUE
        )
        end_time = Sys.time()
        plugin_print(paste0(model_name," training completed after ", 
                            round(end_time - start_time, 1), " seconds"), verbose)
    }
    return(model_list)
}