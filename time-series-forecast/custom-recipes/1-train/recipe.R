# WORK IN PROGRESS
library(dataiku)
library(R.utils)
R_lib_path <- paste(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastUtils.R", sep="/")
source(R_lib_path)

input_dataset_name = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
TIME_COLUMN <- config[["TIME_COLUMN"]]
SERIES_COLUMN <- config[["SERIES_COLUMN"]]
CHOSEN_GRANULARITY <- config[["CHOSEN_GRANULARITY"]]
TIMEZONE <- config[["TIMEZONE"]]
MAP_TEST <- as.list(config[["MAP_TEST"]])
print(MAP_TEST)

NAIVE_MODEL_ACTIVATED <- TRUE
NAIVE_MODEL_SETTINGS <- "drift"
# c("simple","seasonal","drift")

ARIMA_MODEL_ACTIVATED <- TRUE
ARIMA_MODEL_SETTINGS <- list()

SEASONALTREND_MODEL_ACTIVATED <- TRUE
SEASONALTREND_MODEL_SETTINGS <- list()

SEASONALTREND_MODEL_ACTIVATED <- TRUE
SEASONALTREND_MODEL_SETTINGS <- list()

STATE_SPACE_MODEL_ACTIVATED <- TRUE
STATESPACE_MODEL_SETTINGS <- list()

NEURAL_NETWORK_MODEL_ACTIVATED <- TRUE
NEURAL_NETWORK_MODEL_SETTINGS <- list()

msts <- dkuReadDataset(input_dataset_name,
                     columns = c(TIME_COLUMN, SERIES_COLUMN),
                     colClasses = c("character","numeric")) %>%

        # rename columns to simplify internal handling
        rename("time_column" := !!TIME_COLUMN, "series_column" := !!SERIES_COLUMN) %>%

        # convert to R POSIX date format
        mutate_at(c("time_column"), funs(as.POSIXct(., TIMEZONE, format=dku_date_format))) %>%

        # convert to R multi-seasonal time series format from the forecast package
        msts_conversion(., CHOSEN_GRANULARITY)




if(NAIVE_MODEL_ACTIVATED){
    print("naive_model")
    naive_model <- naive_forecast(msts, NAIVE_MODEL_SETTINGS)
    print(summary(naive_model))
}


if(ARIMA_MODEL_ACTIVATED){
    print("arima_model")
    arima_model <- doCall("auto.arima", y=msts, args=ARIMA_MODEL_SETTINGS, .ignoreUnusedArgs=TRUE)
    print(summary(arima_model))
}


if(SEASONALTREND_MODEL_ACTIVATED){
    print("seasonaltrend_model")
    seasonaltrend_model <- doCall("forecast", object=msts, args=SEASONALTREND_MODEL_SETTINGS, .ignoreUnusedArgs=TRUE)
    print(summary(seasonaltrend_model))
}


if(STATE_SPACE_MODEL_ACTIVATED){
    print("state_space_model")
    state_space_model <- doCall("tbats", y=msts, args=STATESPACE_MODEL_SETTINGS, .ignoreUnusedArgs=TRUE)
    print(summary(state_space_model))
}


if(NEURAL_NETWORK_MODEL_ACTIVATED){
    print("neural_network_model")
    neural_network_model <- doCall("nnetar", y=msts, args=NEURAL_NETWORK_MODEL_SETTINGS, .ignoreUnusedArgs=TRUE)
    print(summary(neural_network_model))
}



