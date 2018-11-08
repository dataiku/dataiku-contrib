# Functions used for the train and evaluate recipe

library(forecast)
library(prophet)
library(R.utils)
library(dataiku)
library(dplyr)
library(tibble)

source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))

predict_forecasting_models <- function(ts, df, model_list, eval_horizon, granularity, conf_interval = 95) {
    forecast_df_list <- list()
    date_range <- tail(seq(max(df$ds), by = granularity, length = eval_horizon + 1), -1)
    for(model_name in names(model_list)){
        model <- model_list[[model_name]]
        if(model_name == "PROPHET_MODEL"){
            freq <- ifelse(granularity == "hour", 3600, granularity)
            future <- make_future_dataframe(model, eval_horizon, freq, include_history = FALSE)
            model$interval.width <- conf_interval / 100.0
            forecast_df_list[[model_name]] <- stats::predict(model, future) %>%
                select_(.dots = c("ds", "yhat", "yhat_lower", "yhat_upper"))
            forecast_df_list[[model_name]]$ds <- date_range # harmonizes dates with other model types
        } else {
            # TODO add special cases for naive and seasonal trend model which cannot use forecast(model, h)
            # forecast is not very consistent in its way of working :/
            model$h <- eval_horizon
            model$level <- conf_interval
            f <- forecast(model, h = eval_horizon, level = conf_interval)
            forecast_df_list[[model_name]] <- tibble(
                ds = date_range,
                yhat = as.numeric(f$mean)[1:eval_horizon],
                yhat_lower = as.numeric(f$lower)[1:eval_horizon],
                yhat_upper = as.numeric(f$upper)[1:eval_horizon],
            )
         }
    }
    return(forecast_df_list)
}

eval_forecasting_df <- function(forecast_df_list, eval_df){
    df_list <- list()
    for(model_name in names(forecast_df_list)){
        forecast_df <- forecast_df_list[[model_name]]
        df_list[[model_name]] <- as.data.frame(forecast::accuracy(f = forecast_df$yhat, x = eval_df$y))
        df_list[[model_name]]$model <- model_name
    }
    df_eval <- do.call("rbind", df_list) %>%
        select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE"))
    rownames(df_eval) <- NULL
    return(df_eval)
}