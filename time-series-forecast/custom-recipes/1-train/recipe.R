# WORK IN PROGRESS
library(dataiku)
R_lib_path <- paste(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastUtils.R", sep="/")
source(R_lib_path)

input_dataset_name = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
output_folder_name = dkuCustomRecipeOutputNamesForRole('output_folder')[1]

config = dkuCustomRecipeConfig()
TIME_COLUMN <- config[["TIME_COLUMN"]]
SERIES_COLUMN <- config[["SERIES_COLUMN"]]
CHOSEN_GRANULARITY <- config[["CHOSEN_GRANULARITY"]]
TIMEZONE <- config[["TIMEZONE"]]

plugin_print("Preparation stage: date parsing and conversion to R time series format")

msts <- dkuReadDataset(input_dataset_name,
                     columns = c(TIME_COLUMN, SERIES_COLUMN),
                     colClasses = c("character","numeric")) %>%

        # rename columns to simplify internal handling
        rename("time_column" := !!TIME_COLUMN, "series_column" := !!SERIES_COLUMN) %>%

        # convert to R POSIX date format
        mutate_at(c("time_column"), funs(as.POSIXct(., TIMEZONE, format=dku_date_format))) %>%

        # convert to R multi-seasonal time series format from the forecast package
        msts_conversion(., CHOSEN_GRANULARITY)

# manage Box Cox time series transformation according to https://otexts.org/fpp2/transformations.html
BOX_COX_TRANSFORMATION_ACTIVATED <- config[["BOX_COX_TRANSFORMATION_ACTIVATED"]]
.LAMBDA <- NULL # internal parameter used by models
if(BOX_COX_TRANSFORMATION_ACTIVATED){
    .LAMBDA <- "auto"
}

# internal parameter for model types currently supported
.MODEL_LIST <- list(
    naive = NULL,
    arima = NULL,
    statespace = NULL,
    seasonaltrend = NULL,
    neuralnetwork = NULL
)

plugin_print("Forecasting stage: training models")

# Naive model
NAIVE_MODEL_ACTIVATED <- config[["NAIVE_MODEL_ACTIVATED"]]
NAIVE_MODEL_METHOD <- config[["NAIVE_MODEL_METHOD"]] # c("simple","seasonal","drift")

if(NAIVE_MODEL_ACTIVATED){
    plugin_print("Naive model training started")
    .MODEL_LIST[["naive"]] <- doCall("naive_forecast", 
                          y = msts, 
                          method = NAIVE_MODEL_METHOD,
                          biasadj = BOX_COX_TRANSFORMATION_ACTIVATED,
                          lambda = .LAMBDA,
                          .ignoreUnusedArgs = TRUE) 
    plugin_print("Naive model training completed")
    #print(summary(.MODEL_LIST[["naive"]]))
}


# Seasonal trend model
SEASONALTREND_MODEL_ACTIVATED <- config[["SEASONALTREND_MODEL_ACTIVATED"]]
SEASONALTREND_MODEL_ERROR_TYPE <- config[["SEASONALTREND_MODEL_ERROR_TYPE"]]
SEASONALTREND_MODEL_TREND_TYPE <- config[["SEASONALTREND_MODEL_TREND_TYPE"]]
SEASONALTREND_MODEL_SEASONALITY_TYPE <- config[["SEASONALTREND_MODEL_SEASONALITY_TYPE"]]
.SEASONALTREND_MODEL_TYPE <- paste(SEASONALTREND_MODEL_ERROR_TYPE, 
                                  SEASONALTREND_MODEL_TREND_TYPE, 
                                  SEASONALTREND_MODEL_SEASONALITY_TYPE,
                                 sep = "")
SEASONALTREND_MODEL_KWARGS <- clean_list_mixed_type(as.list(config[["SEASONALTREND_MODEL_KWARGS"]]))

if(SEASONALTREND_MODEL_ACTIVATED){
    plugin_print("Seasonal trend model training started")    
   if(length(SEASONALTREND_MODEL_KWARGS) != 0){
        plugin_print("Additional parameters are below")
        print(SEASONALTREND_MODEL_KWARGS)
    }
    print(SEASONALTREND_MODEL_KWARGS)
    .MODEL_LIST[["seasonaltrend"]] <- doCall("forecast",
                                  object = msts,
                                  model = .SEASONALTREND_MODEL_TYPE,
                                  biasadj = BOX_COX_TRANSFORMATION_ACTIVATED,
                                  lambda = .LAMBDA,
                                  args = SEASONALTREND_MODEL_KWARGS, 
                                  .ignoreUnusedArgs = TRUE)
    plugin_print("Seasonal trend model training completed")
    #print(summary(.MODEL_LIST[["seasonaltrend"]]))
}


# Neural network model
NEURALNETWORK_MODEL_ACTIVATED <- config[["NEURALNETWORK_MODEL_ACTIVATED"]]
NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS <- config[["NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS"]]
NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS <- config[["NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS"]] # auto -1
NEURALNETWORK_MODEL_SIZE <- config[["NEURALNETWORK_MODEL_SIZE"]] # auto -1
NEURALNETWORK_MODEL_KWARGS <- clean_list_mixed_type(as.list(config[["NEURALNETWORK_MODEL_KWARGS"]]))
if(NEURALNETWORK_MODEL_ACTIVATED){  
    if(NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS != -1){
        # -1 is for automatic selection in which case the parameter should not be assigned
        NEURALNETWORK_MODEL_KWARGS[["p"]] <- NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS
    }
    if(NEURALNETWORK_MODEL_SIZE != -1){
        # -1 is for automatic selection in which case the parameter should not be assigned
        NEURALNETWORK_MODEL_KWARGS[["size"]] <- NEURALNETWORK_MODEL_SIZE
    }
    plugin_print("Neural network model training started")
    if(length(NEURALNETWORK_MODEL_KWARGS) != 0){
        plugin_print("Additional parameters are below")
        print(NEURALNETWORK_MODEL_KWARGS)
    }
    # could also be used with external regressors
    .MODEL_LIST[["neuralnetwork"]] <- doCall("nnetar",
                                   y = msts,
                                   P = NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS,
                                   biasadj = BOX_COX_TRANSFORMATION_ACTIVATED,
                                   lambda = .LAMBDA,
                                   args = NEURALNETWORK_MODEL_KWARGS,
                                   .ignoreUnusedArgs = TRUE)
    plugin_print("Neural network model training completed")
    #print(summary(.MODEL_LIST[["neuralnetwork"]]))
}


# ARIMA model
ARIMA_MODEL_ACTIVATED <- config[["ARIMA_MODEL_ACTIVATED"]]
ARIMA_MODEL_STEPWISE_ACTIVATED <- config[["ARIMA_MODEL_STEPWISE_ACTIVATED"]]
ARIMA_MODEL_KWARGS <- clean_list_mixed_type(as.list(config[["ARIMA_MODEL_KWARGS"]]))

if(ARIMA_MODEL_ACTIVATED){
    plugin_print("ARIMA model training started")
    if(length(ARIMA_MODEL_KWARGS) != 0){
        plugin_print("Additional parameters are below")
        print(ARIMA_MODEL_KWARGS)
    }
    # could also be used with external regressors
    .MODEL_LIST[["arima"]] <- doCall("auto.arima",
                          y = msts,
                          stepwise = ARIMA_MODEL_STEPWISE_ACTIVATED,
                          biasadj = BOX_COX_TRANSFORMATION_ACTIVATED,
                          lambda = .LAMBDA,
                          args = ARIMA_MODEL_KWARGS,
                          parallel = TRUE,
                          trace = TRUE,
                          .ignoreUnusedArgs = TRUE)
    plugin_print("ARIMA model training completed")
    #print(summary(.MODEL_LIST[["arima"]]))
}


# State space model
STATE_SPACE_MODEL_ACTIVATED <- config[["STATE_SPACE_MODEL_ACTIVATED"]]
STATESPACE_MODEL_KWARGS <- clean_list_mixed_type(as.list(config[["STATESPACE_MODEL_KWARGS"]]))
if(STATE_SPACE_MODEL_ACTIVATED){
    plugin_print("State space model training started")
    if(length(STATESPACE_MODEL_KWARGS) != 0){
        plugin_print("Additional parameters are below")
        print(STATESPACE_MODEL_KWARGS)
    }
    .MODEL_LIST[["statespace"]] <- doCall("tbats",
                                y = msts, 
                                biasadj = BOX_COX_TRANSFORMATION_ACTIVATED,
                                lambda = .LAMBDA,
                                args = STATESPACE_MODEL_KWARGS,
                                .ignoreUnusedArgs = TRUE)
    plugin_print("State space model training completed")
    #print(summary(.MODEL_LIST[["statespace"]]))
}


plugin_print("All stages completed!")

save_models_to_managed_folder(.MODEL_LIST, output_folder_name)