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

plugin_print("Preparation stage starting...")

df <- dkuReadDataset(input_dataset_name,
                     columns = c(TIME_COLUMN, SERIES_COLUMN),
                     colClasses = c("character","numeric")) 

# convert to R POSIX date format
df[[TIME_COLUMN]] <- as.POSIXct(df[[TIME_COLUMN]], TIMEZONE, format = dku_date_format)

# convert to msts time series format
ts <- msts_conversion(df, TIME_COLUMN, SERIES_COLUMN, GRANULARITY)

plugin_print("Preparation stage completed")

plugin_print("Training stage starting...")

# Bring all model parameters into a standard named list format for all models

NAIVE_MODEL_KWARGS[["method"]] <- NAIVE_MODEL_METHOD

PROPHET_MODEL_KWARGS[["growth"]] <- PROPHET_MODEL_GROWTH
if(PROPHET_MODEL_GROWTH == 'logistic') {
    PROPHET_MODEL_KWARGS[["floor"]] <- PROPHET_MODEL_MINIMUM
    PROPHET_MODEL_KWARGS[["cap"]] <- PROPHET_MODEL_MAXIMUM
} 

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
    NAIVE_MODEL = list(model_function = "dkuNaive"),
    SEASONALTREND_MODEL = list(model_function = "stlf"),
    PROPHET_MODEL = list(model_function = "dkuProphet"),
    ARIMA_MODEL = list(model_function = "auto.arima"),
    EXPONENTIALSMOOTHING_MODEL = list(model_function = "ets"), 
    NEURALNETWORK_MODEL = list(model_function = "nnetar"),
    STATESPACE_MODEL = list(model_function = "tbats")
)

for(model_name in names(model_parameter_list)){
    model_parameter_list[[model_name]][["kwargs"]] <- get(paste0(model_name,"_KWARGS"))
    if(model_name != "PROPHET_MODEL"){
        model_parameter_list[[model_name]][["kwargs"]][["biasadj"]] <- BOX_COX_TRANSFORMATION_ACTIVATED
        if(BOX_COX_TRANSFORMATION_ACTIVATED) model_parameter_list[[model_name]][["kwargs"]][["lambda"]] <- "auto"
    }
}

print(model_parameter_list)

# Now launch training of activated models
model_list <- list()
for(model_name in names(model_parameter_list)){
    model_activated <- get(paste0(model_name,"_ACTIVATED"))
    if(model_activated){
        plugin_print(paste0(model_name," training starting"))
        model_list[[model_name]] <- R.utils::doCall(
            .fcn = model_parameter_list[[model_name]][["model_function"]],
            y = ts,
            args = model_parameter_list[[model_name]][["kwargs"]],
            .ignoreUnusedArgs = TRUE
        )
        plugin_print(paste0(model_name," training completed"))
    }
}

plugin_print("Training stage completed, saving models to output folder")

version_name <- as.character(Sys.time())
save_forecasting_objects(model_folder_name, PARTITION_DIMENSION_NAME, version_name,
                         ts, df, model_parameter_list, model_list)

plugin_print("Models, time series and parameters saved to folder")

#plugin_print("Evaluation stage starting")

#plugin_print("Evaluation stage completed, writing results to output dataset")