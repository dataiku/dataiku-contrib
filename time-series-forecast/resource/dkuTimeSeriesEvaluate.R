# Functions used for the train and evaluate recipe

library(forecast)
library(prophet)
library(R.utils)
library(dataiku)
library(dplyr)
library(tibble)

source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))
source(file.path(dkuCustomRecipeResource(), "dkuTimeSeriesPredict.R"))

eval_forecasting_df_split <- function(forecast_df_list, eval_df){
    df_list <- list()
    for(model_name in names(forecast_df_list)){
        forecast_df <- forecast_df_list[[model_name]]
        df_list[[model_name]] <- as.data.frame(forecast::accuracy(f = forecast_df$yhat, x = eval_df$y))
        df_list[[model_name]]$model <- model_name
    }
    performance_df <- do.call("rbind", df_list) %>%
        select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE"))
    rownames(performance_df) <- NULL
    return(performance_df)
}



eval_forecasting_df_crossval <- function(forecast_df_list, eval_df){
    #TODO 
}



eval_models <- function(ts, df, model_parameter_list, model_list, eval_strategy, eval_horizon,  granularity){
    if(eval_strategy == 'split'){
        train_ts <- head(ts, length(ts) - eval_horizon)
        eval_ts <- tail(ts, eval_horizon)
        train_df <- head(df, nrow(df) - eval_horizon)
        eval_df <- tail(df, eval_horizon)

        eval_model_list <- train_forecasting_models(
            train_ts, train_df, model_parameter_list,
            refit = TRUE, refit_model_list = model_list,
            verbose=FALSE
        )

        eval_forecast_df_list <- predict_forecasting_models(
             train_ts, train_df, eval_model_list, model_parameter_list, eval_horizon, granularity)

        eval_performance_df <- eval_forecasting_df_split(eval_forecast_df_list, eval_df)

    } else if(eval_strategy == 'crossval') {
        # TODO
        stop("Crossval strategy not implemented yet, please choose split for now")
    }
    eval_performance_df[["evaluation_horizon"]] <- as.integer(eval_horizon)
    eval_performance_df[["evaluation_period"]] <- paste0(granularity,"s")
    eval_performance_df[["evaluation_strategy"]] <- eval_strategy
    return(eval_performance_df)
}
