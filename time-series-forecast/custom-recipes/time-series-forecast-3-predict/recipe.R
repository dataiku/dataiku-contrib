########## LIBRARY LOADING ##########

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "predict.R"))


########## INPUT OUTPUT CONFIGURATION ##########

modelFolderName = dkuCustomRecipeInputNamesForRole('model_folder')[1]
evalDatasetName = dkuCustomRecipeInputNamesForRole('eval_dataset')[1]
outputDatasetName = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)

for(n in names(config)) {
  assign(n, CleanPluginParam(config[[n]]))
}
if (!INCLUDE_FORECAST && !INCLUDE_HISTORY) {
  stop("[ERROR] Please include either forecast and/or history")
}

# Check that partitioning settings are correct if activated
checkPartitioning <- CheckPartitioningSettings(evalDatasetName,
  PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)

# Loads all forecasting objects from the model folder
LoadForecastingObjects(modelFolderName, partitionDimensionName, checkPartitioning)
for(n in names(configTrain)) {
  assign(n, CleanPluginParam(configTrain[[n]]))
}


########## MODEL SELECTION ##########

PrintPlugin("Model selection stage")

if (MODEL_SELECTION == "auto") {
  evalDf <- dkuReadDataset(evalDatasetName, columns = c("model", ERROR_METRIC))
  SELECTED_MODEL <- evalDf[[which.min(evalDf[[ERROR_METRIC]]), "model"]]
} 

PrintPlugin(paste0(SELECTED_MODEL, " selected"))


########## FORECASTING STAGE ##########

PrintPlugin("Forecasting stage")

forecastDfList <- GetForecasts(
  ts, df, 
  modelList[SELECTED_MODEL], 
  modelParameterList[SELECTED_MODEL], 
  FORECAST_HORIZON, 
  GRANULARITY,
  CONFIDENCE_INTERVAL,
  INCLUDE_HISTORY
)

forecastDf <- forecastDfList[[SELECTED_MODEL]]

dfOutput <- CombineForecastHistory(df, forecastDf, INCLUDE_FORECAST, INCLUDE_HISTORY)
dfOutput[["selected_model"]] <- SELECTED_MODEL

# Standardises column names
names(dfOutput) <- dplyr::recode(
  .x = names(dfOutput),
  ds = TIME_COLUMN,
  y = SERIES_COLUMN,
  yhat = "forecast_mean",
  yhat_lower = "forecast_lower_confidence_interval",
  yhat_upper = "forecast_upper_confidence_interval",
  residuals = "forecast_residuals"
)

# converts the date from POSIX to a character following dataiku date format in ISO 8601 standard
dfOutput[[TIME_COLUMN]] <- strftime(dfOutput[[TIME_COLUMN]] , dkuDateFormat)

# removes the unnecessary floor and cap columns from prophet model if they exist
dfOutput <- dfOutput %>% 
  select(-one_of(c("floor", "cap")))

# Recipe outputs
WriteDatasetWithPartitioningColumn(dfOutput, outputDatasetName,
  PARTITION_DIMENSION_NAME, checkPartitioning)

PrintPlugin("All stages completed!")