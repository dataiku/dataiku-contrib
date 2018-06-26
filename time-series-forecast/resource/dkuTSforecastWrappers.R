# Collection of wrapper functions to forecast methods
# It works but it could be factorized into a single simple function or class

library(forecast)
library(R.utils)

naive_model_train <- function(ts, method, lambda, biasadj) {
    #' Wrap naive models from the forecast package in a simpler standard way
    #'
    #' @description Depending on a simple "method" argument, this wrapper function switches to 
    #' different implementations of naive models in the forecast package.
    
    plugin_print("Naive model training started")   
    
    model <- switch(method,
        simple = forecast::naive(ts, lambda = lambda, biasadj = biasadj),
        seasonal = forecast::snaive(ts, lambda = lambda, biasadj = biasadj),
        drift = forecast::rwf(ts, drift=TRUE, lambda = lambda, biasadj = biasadj)
    )
    
    plugin_print("Naive model training completed")
    
    return(model)
}

naive_forecast_function <- function(ts, h){
    switch(NAIVE_MODEL_METHOD,
        simple = forecast::naive(ts, h = h, lambda = .LAMBDA, biasadj = BIASADJ),
        seasonal = forecast::snaive(ts, h = h, lambda = .LAMBDA, biasadj = BIASADJ),
        drift = forecast::rwf(ts, h = h, drift = TRUE, lambda = .LAMBDA, biasadj = BIASADJ)
    )
}


seasonaltrend_model_train <- function(ts, error_type, trend_type, seasonality_type, lambda, biasadj, kwargs) {
    #' Wrap seasonal trend models from the forecast package in a simpler standard way

    plugin_print("Seasonal trend model training started") 
    
    model_type <- paste0(error_type, trend_type, seasonality_type)
    model <- doCall("forecast", 
        object = ts,
        model = model_type,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("Seasonal trend model training completed")
    
    return(model)
}

seasonal_trend_forecast_function <- function(ts, h){
    model_type <- substr(SEASONALTREND_MODEL$method, 1, 3) # ETS or STL
    switch(model_type,
        ETS = forecast(ts, h = h, model = ets(ts)),
        STL = forecast(ts, h = h, model = stlf(ts)),
    )
}


neuralnetwork_model_train <- function(ts, non_seasonal_lags, seasonal_lags, size, lambda, biasadj, kwargs) {
    #' Wrap seasonal trend models from the forecast package in a simpler standard way

    if(non_seasonal_lags != -1){
        # -1 is for automatic selection in which case the parameter should not be assigned
        kwargs[["p"]] <- non_seasonal_lags
    }
    if(size != -1){
        # -1 is for automatic selection in which case the parameter should not be assigned
        kwargs[["size"]] <- size
    }
    
    plugin_print("Neural network model training started")
    
    # could also be used with external regressors
    model <- doCall("nnetar", 
        y = ts,
        P = seasonal_lags,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("Neural network model training completed")
    
    return(model)
}

neuralnetwork_forecast_function <- function(ts, h){
    forecast(ts, h = h, model = nnetar(ts, model = NEURALNETWORK_MODEL))
}

arima_model_train <- function(ts, stepwise, lambda, biasadj, kwargs) {
    #' Wrap auto.arima models from the forecast package in a simpler standard way

    plugin_print("ARIMA model training started") 
    
    # could also be used with external regressors
    model <- doCall("auto.arima", 
        y = ts,
        stepwise = stepwise,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        parallel = TRUE,
        trace = TRUE,
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("ARIMA model training completed")
    
    return(model)
}

arima_forecast_function <- function(ts, h){
    forecast(ts, h = h, model = Arima(ts, model = ARIMA_MODEL))
}

statespace_model_train <- function(ts, lambda, biasadj, kwargs) {
    #' Wrap tbats models from the forecast package in a simpler standard way

    plugin_print("State model training started") 
    
    # could also be used with external regressors
    model <- doCall("tbats", 
        y = ts,
        lambda = lambda,
        biasadj = biasadj,
        args = kwargs, 
        .ignoreUnusedArgs = TRUE
    )
    
    plugin_print("State model model training completed")
    
    return(model)
}

statespace_forecast_function <- function(ts, h){
    forecast(ts, h = h, model = tbats(ts, model = STATESPACE_MODEL))
}