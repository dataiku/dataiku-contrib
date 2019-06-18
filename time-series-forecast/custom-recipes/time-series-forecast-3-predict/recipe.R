##### LIBRARY LOADING #####

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "predict.R"))


##### INPUT OUTPUT CONFIGURATION #####

MODEL_FOLDER_NAME <- dkuCustomRecipeInputNamesForRole('MODEL_FOLDER_NAME')[1]
EVALUATION_DATASET_NAME <- dkuCustomRecipeInputNamesForRole('EVALUATION_DATASET_NAME')[1]
FUTURE_XREG_DATASET_NAME <- dkuCustomRecipeInputNamesForRole('FUTURE_XREG_DATASET_NAME')[1]
OUTPUT_DATASET_NAME <- dkuCustomRecipeOutputNamesForRole('OUTPUT_DATASET_NAME')[1]

config = dkuCustomRecipeConfig()
for(n in names(config)) {
  assign(n, CleanPluginParam(config[[n]]))
}
if (!INCLUDE_FORECAST && !INCLUDE_HISTORY) {
  PrintPlugin("Please include either forecast and/or history", stop = TRUE)
}

# Check that partitioning settings are correct if activated
checkPartitioning <- CheckPartitioningSettings(EVALUATION_DATASET_NAME)

# Loads all forecasting objects from the model folder
LoadForecastingObjects(MODEL_FOLDER_NAME)
for(n in names(configTrain)) {
  assign(n, CleanPluginParam(configTrain[[n]]))
}


##### MODEL SELECTION #####

PrintPlugin("Model selection stage")

if (MODEL_SELECTION == "auto") {
  evalDf <- dkuReadDataset(EVALUATION_DATASET_NAME, columns = c("model", ERROR_METRIC))
  SELECTED_MODEL <- evalDf[[which.min(evalDf[[ERROR_METRIC]]), "model"]] %>%
    recode(!!!MODEL_UI_NAME_LIST_REV)
}

PrintPlugin(paste0("Model selection stage completed: ", SELECTED_MODEL, " selected."))


##### FORECASTING STAGE #####

PrintPlugin("Forecasting stage")

externalRegressorMatrix <- NULL
if (!is.na(FUTURE_XREG_DATASET_NAME)) {
  if (is.null(EXT_SERIES_COLUMNS) || length(EXT_SERIES_COLUMNS) == 0 || is.na(EXT_SERIES_COLUMNS)) {
    PrintPlugin("Future external regressors dataset provided but no external regressors \
                were provided at training time. Please re-run the Train and Evaluate recipe \
                with external regressors specified in the recipe settings.", stop = TRUE)
  }
  PrintPlugin("Including the future values of external regressors")
  selectedColumns <- c(TIME_COLUMN, EXT_SERIES_COLUMNS)
  columnClasses <- c("character", rep("numeric", length(EXT_SERIES_COLUMNS)))
  dfXreg <- dkuReadDataset(FUTURE_XREG_DATASET_NAME, columns = selectedColumns, colClasses = columnClasses) %>%
    PrepareDataframeWithTimeSeries(TIME_COLUMN, EXT_SERIES_COLUMNS,
      GRANULARITY, AGGREGATION_STRATEGY, resample = FALSE)
  FORECAST_HORIZON <- nrow(dfXreg)
  externalRegressorMatrix <- as.matrix(dfXreg[EXT_SERIES_COLUMNS])
  colnames(externalRegressorMatrix) <- EXT_SERIES_COLUMNS
} else {
  if(length(EXT_SERIES_COLUMNS) != 0) {
    PrintPlugin("External regressors were used at training time but \
                no dataset for future values of regressors has been provided. \
                Please add the dataset for future values in the Input / Output tab of the recipe. \
                If no future values are availables, please re-run the Train and Evaluate recipe \
                without external regressors", stop = TRUE)
  }
}

forecastDfList <- GetForecasts(
  ts, df, externalRegressorMatrix,
  modelList[SELECTED_MODEL],
  modelParameterList[SELECTED_MODEL],
  FORECAST_HORIZON,
  GRANULARITY,
  CONFIDENCE_INTERVAL,
  INCLUDE_HISTORY
)

forecastDf <- forecastDfList[[SELECTED_MODEL]]

dfOutput <- CombineForecastHistory(df[c("ds", "y")], forecastDf, INCLUDE_FORECAST, INCLUDE_HISTORY)
dfOutput[["selected_model"]] <- recode(SELECTED_MODEL, !!!MODEL_UI_NAME_LIST)

# Standardises column names
names(dfOutput) <- dplyr::recode(
  .x = names(dfOutput),
  ds = TIME_COLUMN,
  y = SERIES_COLUMN,
  yhat = "forecast",
  yhat_lower = "forecast_lower_confidence_interval",
  yhat_upper = "forecast_upper_confidence_interval",
  residuals = "forecast_residuals"
)

# converts the date from POSIX to a character following dataiku date format in ISO 8601 standard
dfOutput[[TIME_COLUMN]] <- strftime(dfOutput[[TIME_COLUMN]] , dkuDateFormat)

# # removes the unnecessary floor and cap columns from prophet model if they exist
# dfOutput <- dfOutput %>%
#   select(-one_of(c("floor", "cap")))

PrintPlugin("Forecasting stage completed, saving forecasts to output dataset.")

WriteDatasetWithPartitioningColumn(dfOutput, OUTPUT_DATASET_NAME)

PrintPlugin("All stages completed!")
