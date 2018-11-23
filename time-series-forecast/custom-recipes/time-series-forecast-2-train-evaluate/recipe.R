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
checkPartition <- CheckPartitioningSettings(inputDatasetName,
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
if (NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS != -1) {
  NEURALNETWORK_MODEL_KWARGS[["p"]] <- NEURALNETWORK_MODEL_NUMBER_NON_SEASONAL_LAGS
} 
if (NEURALNETWORK_MODEL_SIZE != -1) {
  NEURALNETWORK_MODEL_KWARGS[["size"]] <- NEURALNETWORK_MODEL_SIZE
}  
if (CROSSVAL_INITIAL == -1) {
  CROSSVAL_INITIAL <- 10 * EVAL_HORIZON
}
if (CROSSVAL_PERIOD == -1) {
  CROSSVAL_PERIOD <- ceiling(0.5 * EVAL_HORIZON)
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


########## DATA PREPARATION STAGE ##########

PrintPlugin("Preparation stage starting...")

df <- dkuReadDataset(inputDatasetName, columns = c(TIME_COLUMN, SERIES_COLUMN), 
  colClasses = c("character","numeric")) 

# convert to R POSIX date format
df[[TIME_COLUMN]] <- as.POSIXct(df[[TIME_COLUMN]], format = dkuDateFormat)

# truncate all dates to the start of the period to avoid errors at later stages
df[[TIME_COLUMN]] <- TruncateDate(df[[TIME_COLUMN]], GRANULARITY)

dateRange <- seq(min(df[[TIME_COLUMN]]), max(df[[TIME_COLUMN]]), by = GRANULARITY)
if (length(dateRange) != nrow(df)) {
  stop(paste0("[ERROR] Data must be sampled at regular ", GRANULARITY, "ly granularity"))
}

if (EVAL_STRATEGY == "crossval" && (EVAL_HORIZON + CROSSVAL_INITIAL > nrow(df))) {
  stop(paste("[ERROR] Less data than horizon after initial cross-validation window.", 
    "Make horizon or initial shorter."))
}

# convert to msts time series format
ts <- ConvertDFtoTS(df, TIME_COLUMN, SERIES_COLUMN, GRANULARITY)

# convert df to generic prophet-compatible format
names(df) <- c('ds','y')
if (PROPHET_MODEL_ACTIVATED && PROPHET_MODEL_GROWTH == 'logistic') {
  df[['floor']] <- PROPHET_MODEL_MINIMUM
  df[['cap']] <- PROPHET_MODEL_MAXIMUM
}

PrintPlugin("Preparation stage completed")


########## TRAINING STAGE ##########

PrintPlugin("Training stage starting...")

modelList <- TrainForecastingModels(ts, df, modelParameterList)

PrintPlugin("Training stage completed, saving models to output folder")

versionName <- as.character(Sys.time())
configTrain <- config
SaveForecastingObjects(
  folderName = modelFolderName,
  partitionDimensionName = PARTITION_DIMENSION_NAME,
  versionName = versionName, 
  ts, df, modelParameterList, modelList, configTrain
) 

PrintPlugin("Models, time series and parameters saved to folder")


########## EVALUATION STAGE ##########

PrintPlugin(paste0("Evaluation stage starting with ", EVAL_STRATEGY, " strategy..."))

performanceDF <- EvaluateModels(ts, df, modelList, modelParameterList, 
  EVAL_STRATEGY, EVAL_HORIZON,  GRANULARITY, CROSSVAL_PERIOD, CROSSVAL_INITIAL) %>%
  mutate_all(funs(ifelse(is.infinite(.), NA, .)))

performanceDF[["training_date"]] <- strftime(versionName, dkuDateFormat)

PrintPlugin("Evaluation stage completed, saving evaluation results to output dataset")

# Recipe outputs
WriteDatasetWithPartitioningColumn(performanceDF, evalDatasetName, 
  PARTITION_DIMENSION_NAME, checkPartitioning)

PrintPlugin("All stages completed!")