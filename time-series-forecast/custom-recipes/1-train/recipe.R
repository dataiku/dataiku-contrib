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

ts <- dkuReadDataset(input_dataset_name,
                     columns = c(TIME_COLUMN, SERIES_COLUMN),
                     colClasses = c("character","numeric")) %>%

        # rename columns to simplify internal handling
        rename("time_column" := !!TIME_COLUMN, "series_column" := !!SERIES_COLUMN) %>%

        # convert to R POSIX date format
        mutate_at(c("time_column"), funs(as.POSIXct(., TIMEZONE, format=dku_date_format))) %>%

        # convert to R multi-seasonal time series format from the forecast package
        msts_conversion(., CHOSEN_GRANULARITY)

# manage Box Cox time series transformation according to https://otexts.org/fpp2/transformations.html
BIASADJ <- config[["BOX_COX_TRANSFORMATION_ACTIVATED"]]
.LAMBDA <- NULL # internal parameter used by models
if(BIASADJ){
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
NAIVE_MODEL_METHOD <- config[["NAIVE_MODEL_METHOD"]] 

if(NAIVE_MODEL_ACTIVATED){
    .MODEL_LIST[["naive"]] <- naive_model_train(
        ts = ts, 
        method = NAIVE_MODEL_METHOD,
        lambda = .LAMBDA,
        biasadj = BIASADJ
    )
    #print(summary(.MODEL_LIST[["naive"]]))
}


# Seasonal trend model
SEASONALTREND_MODEL_ACTIVATED <- config[["SEASONALTREND_MODEL_ACTIVATED"]]
SEASONALTREND_MODEL_ERROR_TYPE <- config[["SEASONALTREND_MODEL_ERROR_TYPE"]]
SEASONALTREND_MODEL_TREND_TYPE <- config[["SEASONALTREND_MODEL_TREND_TYPE"]]
SEASONALTREND_MODEL_SEASONALITY_TYPE <- config[["SEASONALTREND_MODEL_SEASONALITY_TYPE"]]
SEASONALTREND_MODEL_KWARGS <- clean_kwargs_from_param(config[["SEASONALTREND_MODEL_KWARGS"]])

if(SEASONALTREND_MODEL_ACTIVATED){
    .MODEL_LIST[["seasonaltrend"]] <- seasonaltrend_model_train(
        ts = ts, 
        error_type = SEASONALTREND_MODEL_ERROR_TYPE,
        trend_type = SEASONALTREND_MODEL_TREND_TYPE,
        seasonality_type = SEASONALTREND_MODEL_SEASONALITY_TYPE,
        lambda = .LAMBDA,
        biasadj = BIASADJ,
        kwargs = SEASONALTREND_MODEL_KWARGS
    )
    #print(summary(.MODEL_LIST[["seasonaltrend"]]))
}


# Neural network model
NEURALNETWORK_MODEL_ACTIVATED <- config[["NEURALNETWORK_MODEL_ACTIVATED"]]
NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS <- config[["NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS"]]
NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS <- config[["NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS"]] # auto -1
NEURALNETWORK_MODEL_SIZE <- config[["NEURALNETWORK_MODEL_SIZE"]] # auto -1
NEURALNETWORK_MODEL_KWARGS <- clean_kwargs_from_param(config[["NEURALNETWORK_MODEL_KWARGS"]])

if(NEURALNETWORK_MODEL_ACTIVATED){  
    .MODEL_LIST[["seasonaltrend"]] <- neuralnetwork_model_train(
        ts = ts, 
        non_seasonal_lags = NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS,
        seasonal_lags = NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS,
        size = NEURALNETWORK_MODEL_SIZE,
        lambda = .LAMBDA,
        biasadj = BIASADJ,
        kwargs = NEURALNETWORK_MODEL_KWARGS
    )
    #print(summary(.MODEL_LIST[["neuralnetwork"]]))
}


# ARIMA model
ARIMA_MODEL_ACTIVATED <- config[["ARIMA_MODEL_ACTIVATED"]]
ARIMA_MODEL_STEPWISE_ACTIVATED <- config[["ARIMA_MODEL_STEPWISE_ACTIVATED"]]
ARIMA_MODEL_KWARGS <- clean_kwargs_from_param(config[["ARIMA_MODEL_KWARGS"]])

if(ARIMA_MODEL_ACTIVATED){
    .MODEL_LIST[["arima"]] <- arima_model_train(
        ts = ts,
        stepwise = ARIMA_MODEL_STEPWISE_ACTIVATED,
        lambda = .LAMBDA,
        biasadj = BIASADJ,
        kwargs = ARIMA_MODEL_KWARGS
    )
    #print(summary(.MODEL_LIST[["arima"]]))
}


# State space model
STATE_SPACE_MODEL_ACTIVATED <- config[["STATE_SPACE_MODEL_ACTIVATED"]]
STATESPACE_MODEL_KWARGS <- clean_kwargs_from_param(config[["STATESPACE_MODEL_KWARGS"]])

if(STATE_SPACE_MODEL_ACTIVATED){
    .MODEL_LIST[["statespace"]] <- statespace_model_train(
        ts = ts, 
        lambda = .LAMBDA,
        biasadj = BIASADJ,
        kwargs = STATESPACE_MODEL_KWARGS
    )
    #print(summary(.MODEL_LIST[["statespace"]]))
}


plugin_print("All stages completed!")

save_to_managed_folder(
    folder_id = output_folder_name,
    model_list = .MODEL_LIST, 
    msts,
    TIME_COLUMN,
    SERIES_COLUMN,
    CHOSEN_GRANULARITY,
    TIMEZONE
)