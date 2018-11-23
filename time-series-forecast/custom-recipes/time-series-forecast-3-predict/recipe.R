########## LIBRARY LOADING ##########

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "predict.R"))


########## INPUT OUTPUT CONFIGURATION ##########

model_folder_name = dkuCustomRecipeInputNamesForRole('model_folder')[1]
eval_dataset_name = dkuCustomRecipeInputNamesForRole('eval_dataset')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)

for(n in names(config)){
    assign(n, clean_plugin_param(config[[n]]))
}
if(!INCLUDE_FORECAST && !INCLUDE_HISTORY){
    stop("[ERROR] Please include either forecast and/or history")
}

# Check that partitioning settings are correct if activated
check_partition <- check_partitioning_settings(eval_dataset_name,
    PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)

# loads all forecasting objects from the model folder
load_forecasting_objects(model_folder_name, partition_dimension_name)
for(n in names(train_config)){
    assign(n, clean_plugin_param(train_config[[n]]))
}


########## MODEL SELECTION ##########

plugin_print("Model selection stage")

if(MODEL_SELECTION == "auto") {
    eval_df <- dkuReadDataset(eval_dataset_name, columns = c("model", ERROR_METRIC))
    SELECTED_MODEL <- eval_df[[which.min(eval_df[[ERROR_METRIC]]), "model"]]
} 
plugin_print(paste0(SELECTED_MODEL, " selected"))


########## FORECASTING STAGE ##########

plugin_print("Forecasting stage")

forecast_df_list <- get_forecasts(
    ts, df, 
    model_list[SELECTED_MODEL], 
    model_parameter_list[SELECTED_MODEL], 
    FORECAST_HORIZON, 
    GRANULARITY,
    CONFIDENCE_INTERVAL,
    INCLUDE_HISTORY
)

forecast_df <- forecast_df_list[[SELECTED_MODEL]]

df_output <- combine_forecast_history(df, forecast_df, INCLUDE_FORECAST, INCLUDE_HISTORY)

plugin_print("All stages completed, writing forecast and/or residuals to output dataset")


########## OUTPUT FORMATTING STAGE ##########

names(df_output) <- dplyr::recode(
    .x = names(df_output),
    ds = TIME_COLUMN,
    y = SERIES_COLUMN,
    yhat = "forecast_mean",
    yhat_lower = "forecast_lower_confidence_interval",
    yhat_upper = "forecast_upper_confidence_interval",
    residuals = "forecast_residuals"
)

df_output[["selected_model"]] <- SELECTED_MODEL

# converts the date from POSIX to a character following dataiku date format in ISO 8601 standard
df_output[[TIME_COLUMN]] <- strftime(df_output[[TIME_COLUMN]] , dku_date_format)

# removes the unnecessary floor and cap columns from prophet model if they exist
df_output <- df_output %>% 
    select(-one_of(c("floor", "cap")))

# Recipe outputs
write_dataset_with_partitioning_column(df_output, output_dataset_name,
    PARTITION_DIMENSION_NAME, check_partitioning)