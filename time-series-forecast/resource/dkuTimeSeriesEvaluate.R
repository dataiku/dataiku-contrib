# Functions used for the train and evaluate recipe

library(forecast)
library(prophet)
library(R.utils)
library(dataiku)
library(dplyr)
library(tibble)

source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))
source(file.path(dkuCustomRecipeResource(), "dkuTimeSeriesPredict.R"))

ME <- function(df, w) {
   e <- df$y - df$yhat
  return(prophet:::rolling_mean(e, w))
}

MSE <- function(df, w) {
  se <- (df$y - df$yhat) ** 2
  return(prophet:::rolling_mean(se, w))
}

RMSE <- function(df, w) {
  return(sqrt(MSE(df, w)))
}

MAE <- function(df, w) {
  ae <- abs(df$y - df$yhat)
  return(prophet:::rolling_mean(ae, w))
}

MAPE <- function(df, w) {
  ape <- abs((df$y - df$yhat) / df$y)
  return(prophet:::rolling_mean(ape, w))
}

MPE <- function(df, w) {
  pe <- (df$y - df$yhat) / df$y
  return(prophet:::rolling_mean(pe, w))
}

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

crossval_performance_metrics <- function(df, metrics = c("ME", "RMSE", "MAE", "MPE", "MAPE"), rolling_window = 1.0) {
    df_m <- df
    df_m[["horizon"]] <- df_m$ds - df_m$cutoff
    df_m <- df_m[order(df_m$horizon),]
    # Window size
    w <- as.integer(rolling_window * nrow(df_m))
    w <- max(w, 1)
    w <- min(w, nrow(df_m))
    cols <- c('horizon')
    for(metric in metrics) {
        df_m[[metric]] <- get(metric)(df_m, w)
        cols <- c(cols, metric)
    }
    df_m <- df_m[cols]
    return(stats::na.omit(df_m))
}

generate_cutoffs_date <- function(df, horizon, initial, period, granularity) {
    
    units <- paste0(granularity, "s")
    plugin_print(paste0("Cross-validation initial train set at ", initial, " ", units))
    plugin_print(paste0("Cross-validation cutoff period set at ", period, " ", units))
    
    error_msg_too_long <- paste("Less data than horizon after initial window.", 
        "Make horizon or initial shorter.")
    
    if(granularity %in% c("hour", "day", "week")){
        # Taken from the prophet:::generate_cutoffs function
        horizon.dt <- as.difftime(horizon, units = units)
        initial.dt <- as.difftime(initial, units = units)
        period.dt <- as.difftime(period, units = units)
        cutoff <- max(df$ds) - horizon.dt
        result <- c(cutoff)
        while (result[length(result)] >= min(df$ds) + initial.dt) {
            cutoff <- cutoff - period.dt
            if (!any((df$ds > cutoff) & (df$ds <= cutoff + horizon.dt))) {
                closest.date <- max(df$ds[df$ds <= cutoff])
                cutoff <- closest.date - horizon.dt
            }
            result <- c(result, cutoff)
        }
    } 
    else {
        # difftime does not work with monthly, quarterly or yearly data
        # assumes that input data is regularly sampled (checks are implemented earlier)
        if(initial + horizon > nrow(df)) {
            stop(error_msg_too_long)
        }
        dates <- sort(df$ds)
        cutoff_index <- length(dates) - horizon
        cutoff <- dates[cutoff_index]
        result <- c(cutoff)
        while(result[length(result)] >= dates[initial+1]) {
            cutoff_index <- cutoff_index - period
            cutoff <- dates[cutoff_index]
            if(cutoff_index <= 1) {
                result <- c(result, NA)
                break
            }
            if(!any((dates > cutoff) & (dates <= dates[cutoff_index + horizon]))) {
                cutoff <- dates[cutoff_index - horizon] 
            }
            result <- c(result, cutoff)
        }
    }
    
    result <- utils::head(result, -1)

    if (length(result) == 0) {
        stop(error_msg_too_long)
    }
    
    plugin_print(paste("Making", length(result), "forecasts with cutoffs between", 
        result[length(result)], "and", result[1]))

    cutoffs <- rev(result)
    return(cutoffs)
}

eval_forecasting_df_crossval <- function(ts, df, model_list, model_parameter_list,
                                        horizon, granularity, period = NULL, initial = NULL) {
    if(is.null(period)) {
        period <- 0.5 * horizon
    }
    if(is.null(initial)) {
        initial <- 10 * horizon
    }
    
    cutoffs <- generate_cutoffs_date(df, horizon, initial, period, granularity)
    
    crossval_df_list <- list()
    for(model_name in names(model_list)){
        crossval_df_list[[model_name]] <- data.frame()
    }
    
    # compute forecasts for all cutoffs
    for(i in 1:length(cutoffs)) {
        cutoff <- cutoffs[i]
        history.c <- dplyr::filter(df, ds <= cutoff)
        if (nrow(history.c) < 2) {
            stop('Less than two datapoints before cutoff. Increase initial window.')
        }
        history.ts <- head(ts, nrow(history.c))
        plugin_print(paste0("Training cross-validation step ", i ,"/", length(cutoffs), " for cutoff ", cutoffs[i], 
                            ", with ", length(history.ts), " rows in the train set"))
        
        df.predict <- head(dplyr::filter(df, ds > cutoff), horizon)
        
        eval_model_list <- train_forecasting_models(
            history.ts, history.c, model_parameter_list,
            refit = TRUE, refit_model_list = model_list,
            verbose=FALSE
        )
        
        forecast_df_list <- predict_forecasting_models(
            history.ts, history.c, eval_model_list, model_parameter_list, horizon, granularity)

        for(model_name in names(forecast_df_list)){
            df.c <- dplyr::inner_join(df.predict, forecast_df_list[[model_name]], by = "ds") %>%
                select(ds, y, yhat, yhat_lower, yhat_upper)
            df.c$cutoff <- cutoff
            crossval_df_list[[model_name]] <- rbind(crossval_df_list[[model_name]] , df.c)
        }
    }
    
    # now compute performance
    perf_df_list <- list()
    for(model_name in names(model_list)){
        perf_df <- crossval_performance_metrics(crossval_df_list[[model_name]])
        if(nrow(perf_df) > 0) {
            perf_df_list[[model_name]] <- perf_df
            perf_df_list[[model_name]]$model <- model_name
        }
    }
    performance_df <- do.call("rbind", perf_df_list) %>%
        select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE"))
    rownames(performance_df) <- NULL
    return(performance_df)
}

eval_models <- function(ts, df, model_list, model_parameter_list, eval_strategy, 
                        eval_horizon,  granularity, period = NULL, initial = NULL){
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
        eval_performance_df <- eval_forecasting_df_crossval(ts, df, model_list, model_parameter_list,
                                        eval_horizon, granularity, period, initial) 
    }
    eval_performance_df[["evaluation_horizon"]] <- as.integer(eval_horizon)
    eval_performance_df[["evaluation_period"]] <- ifelse(eval_horizon==1, granularity, paste0(granularity,"s"))
    eval_performance_df[["evaluation_strategy"]] <- eval_strategy
    return(eval_performance_df)
}