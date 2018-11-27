########## LIBRARY LOADING ##########

library(dataiku)
source(file.path(dkuCustomRecipeResource(), "train.R"))
source(file.path(dkuCustomRecipeResource(), "evaluate.R"))


########## INPUT OUTPUT CONFIGURATION ##########

inputDatasetName = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
modelFolderName = dkuCustomRecipeOutputNamesForRole('model_folder')[1]
evalDatasetName = dkuCustomRecipeOutputNamesForRole('eval_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)
for(n in names(config)) {
  assign(n, CleanPluginParam(config[[n]]))
}

# Check that partitioning settings are correct if activated
checkPartitioning <- CheckPartitioningSettings(inputDatasetName,
  PARTITIONING_ACTIVATED, PARTITION_DIMENSION_NAME)


# Insert all raw parameters for models from plugin UI into each model KWARGS parameter.
# This facilitates the generic calling of forecasting functions with
# a flexible number of keyword arguments.

# Naive model method, see train plugin library
NAIVE_MODEL_KWARGS[["method"]] <- NAIVE_MODEL_METHOD

# See auto.arima doc in www.rdocumentation.org/packages/forecast/versions/8.3/topics/auto.arima
ARIMA_MODEL_KWARGS[["stepwise"]] <- ARIMA_MODEL_STEPWISE

# See ets doc in https://www.rdocumentation.org/packages/forecast/versions/8.3/topics/ets
EXPONENTIALSMOOTHING_MODEL_KWARGS[["model"]] <- paste0(
  EXPONENTIALSMOOTHING_MODEL_ERROR_TYPE, 
  EXPONENTIALSMOOTHING_MODEL_TREND_TYPE, 
  EXPONENTIALSMOOTHING_MODEL_SEASONALITY_TYPE
) 

# See nnetar doc www.rdocumentation.org/packages/forecast/versions/8.3/topics/nnetar
NEURALNETWORK_MODEL_KWARGS[["P"]] <- NEURALNETWORK_MODEL_NUMBER_SEASONAL_LAGS
if (NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS != -1) {
  NEURALNETWORK_MODEL_KWARGS[["p"]] <- NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS
} 
if (NEURALNETWORK_MODEL_SIZE != -1) {
  NEURALNETWORK_MODEL_KWARGS[["size"]] <- NEURALNETWORK_MODEL_SIZE
} 

# Bring all model parameters into a standard named list format for all models
modelParameterList <- list()
for(modelName in AVAILABLE_MODEL_NAME_LIST) {
  modelActivated <- get(paste0(modelName,"_ACTIVATED"))
  if (modelActivated) {
    modelParameterList[[modelName]] <- MODEL_FUNCTION_NAME_LIST[[modelName]]
    modelParameterList[[modelName]][["kwargs"]] <- as.list(get(paste0(modelName,"_KWARGS")))
  }
}

# Handles default options for the cross-validation evaluation strategy
if (CROSSVAL_INITIAL == -1) {
  CROSSVAL_INITIAL <- 10 * EVAL_HORIZON 
}
if (CROSSVAL_PERIOD == -1) {
  CROSSVAL_PERIOD <- ceiling(0.5 * EVAL_HORIZON)
}


# Reads input dataset
df <- dkuReadDataset(inputDatasetName, columns = c(TIME_COLUMN, SERIES_COLUMN), 
    colClasses = c("character","numeric")) %>%
  PrepareDataframeWithTimeSeries(TIME_COLUMN, SERIES_COLUMN, GRANULARITY, resample = FALSE)
names(df) <- c('ds','y') # Converts df to generic prophet-compatible format
if (PROPHET_MODEL_ACTIVATED && PROPHET_MODEL_GROWTH == 'logistic') {
  df[['floor']] <- PROPHET_MODEL_MINIMUM
  df[['cap']] <- PROPHET_MODEL_MAXIMUM
}

# Additional check on the number of rows of the input for the cross-validation evaluation strategy
if (EVAL_STRATEGY == "crossval" && (EVAL_HORIZON + CROSSVAL_INITIAL > nrow(df))) {
  stop(paste("[ERROR] Less data than horizon after initial cross-validation window.", 
    "Make horizon or initial shorter."))
}

# Converts df to msts time series format
ts <- ConvertDataFrameToTimeSeries(df, "ds", "y", GRANULARITY)


########## TRAINING STAGE ##########

PrintPlugin("Training stage starting...")

modelList <- TrainForecastingModels(ts, df, modelParameterList)

PrintPlugin("Training stage completed, saving models to output folder.")

versionName <- as.character(Sys.time())
configTrain <- config
SaveForecastingObjects(
  folderName = modelFolderName,
  partitionDimensionName = PARTITION_DIMENSION_NAME,
  checkPartitioning = checkPartitioning,
  versionName = versionName, 
  ts, df, modelParameterList, modelList, configTrain
) 


########## EVALUATION STAGE ##########

PrintPlugin(paste0("Evaluation stage starting with ", EVAL_STRATEGY, " strategy..."))

errorDf <- EvaluateModels(ts, df, modelList, modelParameterList, 
  EVAL_STRATEGY, EVAL_HORIZON,  GRANULARITY, CROSSVAL_INITIAL, CROSSVAL_PERIOD)
errorDf[["training_date"]] <- strftime(versionName, dkuDateFormat)

PrintPlugin("Evaluation stage completed, saving evaluation results to output dataset.")

WriteDatasetWithPartitioningColumn(errorDf, evalDatasetName, 
  PARTITION_DIMENSION_NAME, checkPartitioning)

PrintPlugin("All stages completed!")