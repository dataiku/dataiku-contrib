# WORK IN PROGRESS NOT READY

library(dataiku)
source(file.path(dataiku:::dkuCustomRecipeResource(), "dkuTimeSeriesTrain.R"))
source(file.path(dataiku:::dkuCustomRecipeResource(), "dkuTimeSeriesEvaluate.R"))

input_dataset_name = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
model_folder_name = dkuCustomRecipeOutputNamesForRole('model_folder')[1]
eval_dataset_name = dkuCustomRecipeOutputNamesForRole('eval_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)
for(n in names(config)){
    assign(n, clean_plugin_param(config[[n]]))
}

check_partition <- check_partitioning_setting_input_output(input_dataset_name, PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)

plugin_print("Preparation stage starting...")

df <- dkuReadDataset(input_dataset_name,
                     columns = c(TIME_COLUMN, SERIES_COLUMN),
                     colClasses = c("character","numeric")) 

# convert to R POSIX date format
df[[TIME_COLUMN]] <- as.POSIXct(df[[TIME_COLUMN]], format = dku_date_format)

# convert to msts time series format
ts <- msts_conversion(df, TIME_COLUMN, SERIES_COLUMN, GRANULARITY)

# convert df to generic prophet-compatible format
names(df) <- c('ds','y')
if(PROPHET_MODEL_ACTIVATED && PROPHET_MODEL_GROWTH == 'logistic'){
    df[['floor']] <- PROPHET_MODEL_MINIMUM
    df[['cap']] <- PROPHET_MODEL_MAXIMUM
}

plugin_print("Preparation stage completed")

plugin_print("Training stage starting...")

# Bring all model parameters into a standard named list format for all models

NAIVE_MODEL_KWARGS[["method"]] <- NAIVE_MODEL_METHOD

ARIMA_MODEL_KWARGS[["stepwise"]] <- ARIMA_MODEL_STEPWISE

EXPONENTIALSMOOTHING_MODEL_KWARGS[["model"]] <- paste0(
    EXPONENTIALSMOOTHING_MODEL_ERROR_TYPE, 
    EXPONENTIALSMOOTHING_MODEL_TREND_TYPE, 
    EXPONENTIALSMOOTHING_MODEL_SEASONALITY_TYPE
)

NEURALNETWORK_MODEL_KWARGS[["P"]] <- NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS
if(NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS != -1) {
    NEURALNETWORK_MODEL_KWARGS[["p"]] <- NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS
} 
if(NEURALNETWORK_MODEL_SIZE != -1) {
    NEURALNETWORK_MODEL_KWARGS[["size"]] <- NEURALNETWORK_MODEL_SIZE
} 

model_parameter_list <- list(
    NAIVE_MODEL = list(model_function = "naive_model_wrapper"),
    SEASONALTREND_MODEL = list(model_function = "stlf"),
    PROPHET_MODEL = list(model_function = "prophet_model_wrapper"),
    ARIMA_MODEL = list(model_function = "auto.arima"),
    EXPONENTIALSMOOTHING_MODEL = list(model_function = "ets"), 
    NEURALNETWORK_MODEL = list(model_function = "nnetar"),
    STATESPACE_MODEL = list(model_function = "tbats")
)

for(model_name in names(model_parameter_list)){
    model_parameter_list[[model_name]][["kwargs"]] <- get(paste0(model_name,"_KWARGS"))
}

# Now launch training of activated models
model_list <- train_forecasting_models(ts, df, model_parameter_list)

plugin_print("Training stage completed, saving models to output folder")

version_name <- as.character(Sys.time())
save_forecasting_objects(
    folder_name = model_folder_name,
    partition_dimension_name = PARTITION_DIMENSION_NAME,
    version_name = version_name, 
    ts = ts,
    df = df,
    model_parameter_list = model_parameter_list,
    model_list = model_list
) 

plugin_print("Models, time series and parameters saved to folder")

plugin_print(paste0("Evaluation stage starting with ", VALIDATION_STRATEGY, " strategy..."))

if(VALIDATION_STRATEGY == 'split'){
    train_ts <- head(ts, length(ts) - EVAL_HORIZON)
    eval_ts <- tail(ts, EVAL_HORIZON)
    train_df <- head(df, nrow(df) - EVAL_HORIZON)
    eval_df <- tail(df, EVAL_HORIZON)

    eval_model_list <- train_forecasting_models(
        train_ts, train_df, model_parameter_list, refit = TRUE, refit_model_list = model_list)

    eval_forecast_df_list <- predict_forecasting_models(
         train_ts, train_df, eval_model_list, EVAL_HORIZON, GRANULARITY)
    print(eval_forecast_df_list)

    eval_df <- eval_forecasting_df(eval_forecast_df_list, eval_df)
    
} else if(VALIDATION_STRATEGY == 'crossval') {
    # TODO
    plugin_print("Not implemented, TODO")
}
    
plugin_print("Evaluation stage completed, saving evaluation results to output dataset")

# Recipe outputs
write_dataset_with_partitioning_column(eval_df, eval_dataset_name, PARTITION_DIMENSION_NAME, check_partitioning)

plugin_print("All stages completed!")