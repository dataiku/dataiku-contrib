# Functions used for the Predict recipe

library(forecast)
library(R.utils)
library(dataiku)
source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))
source(file.path(dkuCustomRecipeResource(), "dkuTimeSeriesTrain.R"))

load_forecasting_objects <- function(model_folder_name, partition_dimension_name, envir = .GlobalEnv) {
    folder_path <- get_folder_path_with_partitioning(model_folder_name, PARTITION_DIMENSION_NAME)
    last_version_path <- max(list.dirs(file.path(folder_path, "versions"), recursive = FALSE))
    plugin_print(paste0("Loading forecasting objects from path ", last_version_path))
    rdata_path_list <- list.files(
        path = last_version_path,
        pattern = "*.RData",
        full.names = TRUE,
        recursive = TRUE
    )
    for(rdata_path in rdata_path_list){
        load(rdata_path, envir = envir)
    }

}

predict_forecasting_models <- function(ts, df, model_list, model_parameter_list, 
                                       horizon, granularity, confidence_interval = 95, 
                                       include_history = FALSE) {
    forecast_df_list <- list()
    
    if(include_history) {
        date_range <- seq(min(df$ds), by = granularity, length = nrow(df) + horizon)
    } else {
        date_range <- tail(seq(max(df$ds), by = granularity, length = horizon + 1), -1)
    }
    date_range <- trunc_to_granularity_start(date_range, granularity)
        
    for(model_name in names(model_list)){
        model <- model_list[[model_name]]
        if(model_name == "PROPHET_MODEL"){
            freq <- ifelse(granularity == "hour", 3600, granularity)
            future <- make_future_dataframe(model, horizon, freq, include_history = include_history)
            model$interval.width <- confidence_interval / 100.0
            forecast_df <- stats::predict(model, future) %>%
                select_(.dots = c("ds", "yhat", "yhat_lower", "yhat_upper"))
            forecast_df$ds <- date_range # harmonizes dates with other model types
        } else {
            # add special cases for naive and seasonal trend model which cannot use forecast(model, h) 
            # they can only be called directly with a horizon argument
            # forecast is not very consistent in its way of working :/ not all models can be fitted
            if(model_name %in% c("NAIVE_MODEL","SEASONALTREND_MODEL")) {
                f <- R.utils::doCall(
                        .fcn = model_parameter_list[[model_name]][["model_function"]],
                        y = ts,
                        h = horizon,
                        level = c(confidence_interval),
                        args = model_parameter_list[[model_name]][["kwargs"]],
                        .ignoreUnusedArgs = FALSE
                )
            } else if(model_name == "NEURALNETWORK_MODEL") {
                f <- forecast(model, h = horizon, level = c(confidence_interval), PI = TRUE)
            } else {
                model$h <- horizon
                f <- forecast(ts, model = model, h = horizon, level = c(confidence_interval))
            }
            if(include_history) {
                forecast_df <- tibble(
                    ds = date_range,
                    yhat = c(f$fitted, as.numeric(f$mean)[1:horizon]),
                    yhat_lower = c(rep(NA, nrow(df)), as.numeric(f$lower)[1:horizon]),
                    yhat_upper = c(rep(NA, nrow(df)), as.numeric(f$upper)[1:horizon]),
                )
            } else {
                forecast_df <- tibble(
                    ds = date_range,
                    yhat = as.numeric(f$mean)[1:horizon],
                    yhat_lower = as.numeric(f$lower)[1:horizon],
                    yhat_upper = as.numeric(f$upper)[1:horizon],
                )
            }
         }
        forecast_df_list[[model_name]] <- forecast_df
    }
    return(forecast_df_list)
}


combine_forecast_history <- function(history_df = NULL, forecast_df = NULL, 
                                     include_forecast=TRUE, include_history=FALSE) {
    if(include_forecast && include_history) {
        df_output <- merge(history_df, forecast_df, by = "ds", all = TRUE)
        df_output["residuals"] <- df_output["y"] - df_output["yhat"]
        df_output["origin"] <- ifelse(is.na(df_output[["y"]]), "forecast","history")
    } else if(include_forecast && !include_history) {
        df_output <- forecast_df
        df_output["origin"] <- "forecast"
    } else if(!include_forecast && include_history) {
        df_output <- merge(history_df, forecast_df, by = "ds", all.y = FALSE) %>% 
            select_(.dots = c("ds", "y", "yhat"))
        df_output["residuals"] <- df_output["y"] - df_output["yhat"]
        df_output["origin"] <- "history"
    } else {
        df_output <- data.frame()
    }
    return(df_output)
}