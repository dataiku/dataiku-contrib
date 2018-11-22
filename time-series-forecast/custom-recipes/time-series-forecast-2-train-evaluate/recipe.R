########## LIBRARY LOADING ##########

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "train.R"))
source(file.path(dkuCustomRecipeResource(), "evaluate.R"))


########## INPUT OUTPUT CONFIGURATION ##########

input_dataset_name = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
model_folder_name = dkuCustomRecipeOutputNamesForRole('model_folder')[1]
eval_dataset_name = dkuCustomRecipeOutputNamesForRole('eval_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)
for(n in names(config)){
    assign(n, clean_plugin_param(config[[n]]))
}

# Check that partitioning settings are correct if activated
check_partition <- check_partitioning_settings(input_dataset_name,
    PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)

# Prepare all raw parameters from plugin UI
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
if(CROSSVAL_INITIAL == -1) CROSSVAL_INITIAL <- 10 * EVAL_HORIZON
if(CROSSVAL_PERIOD == -1) CROSSVAL_PERIOD <- ceiling(0.5 * EVAL_HORIZON)

# Bring all model parameters into a standard named list format for all models
model_parameter_list <- list()
for(model_name in AVAILABLE_MODEL_NAME_LIST){
    model_activated <- get(paste0(model_name,"_ACTIVATED"))
    if(model_activated){
        model_parameter_list[[model_name]] <- MODEL_FUNCTION_NAME_LIST[[model_name]]
        model_parameter_list[[model_name]][["kwargs"]] <- as.list(get(paste0(model_name,"_KWARGS")))
    }
}


########## DATA PREPARATION STAGE ##########

plugin_print("Preparation stage starting...")

df <- dkuReadDataset(input_dataset_name, columns = c(TIME_COLUMN, SERIES_COLUMN), 
    colClasses = c("character","numeric")) 

# convert to R POSIX date format
df[[TIME_COLUMN]] <- as.POSIXct(df[[TIME_COLUMN]], format = dku_date_format)

# truncate all dates to the start of the period to avoid errors at later stages
df[[TIME_COLUMN]] <- truncate_date(df[[TIME_COLUMN]], GRANULARITY)

date_range <- seq(min(df[[TIME_COLUMN]]), max(df[[TIME_COLUMN]]), by = GRANULARITY)
if(length(date_range) != nrow(df)) {
    stop(paste0("[ERROR] Data must be sampled at regular ", GRANULARITY, "ly granularity"))
}

if(EVAL_STRATEGY == "crossval" && (EVAL_HORIZON + CROSSVAL_INITIAL > nrow(df))) {
    stop(paste("[ERROR] Less data than horizon after initial cross-validation window.", 
        "Make horizon or initial shorter."))
}

# convert to msts time series format
ts <- convert_df_to_ts(df, TIME_COLUMN, SERIES_COLUMN, GRANULARITY)

# convert df to generic prophet-compatible format
names(df) <- c('ds','y')
if(PROPHET_MODEL_ACTIVATED && PROPHET_MODEL_GROWTH == 'logistic'){
    df[['floor']] <- PROPHET_MODEL_MINIMUM
    df[['cap']] <- PROPHET_MODEL_MAXIMUM
}

plugin_print("Preparation stage completed")


########## TRAINING STAGE ##########

plugin_print("Training stage starting...")

model_list <- train_forecasting_models(ts, df, model_parameter_list)

plugin_print("Training stage completed, saving models to output folder")

version_name <- as.character(Sys.time())
train_config <- config
save_forecasting_objects(
    folder_name = model_folder_name,
    partition_dimension_name = PARTITION_DIMENSION_NAME,
    version_name = version_name, 
    ts, df, model_parameter_list, model_list, train_config
) 

plugin_print("Models, time series and parameters saved to folder")


########## EVALUATION STAGE ##########

plugin_print(paste0("Evaluation stage starting with ", EVAL_STRATEGY, " strategy..."))

performance_df <- evaluate_models(ts, df, model_list, model_parameter_list, 
    EVAL_STRATEGY, EVAL_HORIZON,  GRANULARITY, CROSSVAL_PERIOD, CROSSVAL_INITIAL) %>%
    mutate_all(funs(ifelse(is.infinite(.), NA, .)))

performance_df[["training_date"]] <- strftime(version_name, dku_date_format)

plugin_print("Evaluation stage completed, saving evaluation results to output dataset")

# Recipe outputs
write_dataset_with_partitioning_column(performance_df, eval_dataset_name, 
    PARTITION_DIMENSION_NAME, check_partitioning)

plugin_print("All stages completed!")