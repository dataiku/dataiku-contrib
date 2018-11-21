# Functions used for the train and evaluate recipe

source(file.path(dkuCustomRecipeResource(), "train.R"))
source(file.path(dkuCustomRecipeResource(), "predict.R"))

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

compute_performance_metrics_split <- function(forecast_df_list, history_df){
    df_list <- list()
    for(model_name in names(forecast_df_list)){
        forecast_df <- forecast_df_list[[model_name]]
        df_list[[model_name]] <- as.data.frame(forecast::accuracy(f = forecast_df$yhat, x = history_df$y))
        df_list[[model_name]]$model <- model_name
    }
    performance_df <- do.call("rbind", df_list) %>%
        select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE"))
    rownames(performance_df) <- NULL
    return(performance_df)
}

evaluate_models_split <- function(ts, df, model_list, model_parameter_list, horizon, granularity) {
    train_ts <- head(ts, length(ts) - horizon)
    eval_ts <- tail(ts, horizon)
    train_df <- head(df, nrow(df) - horizon)
    eval_df <- tail(df, horizon)
    eval_model_list <- train_forecasting_models(
        train_ts, train_df, model_parameter_list,
        refit = TRUE, refit_model_list = model_list,
        verbose=FALSE
    )
    eval_forecast_df_list <- get_forecasts(train_ts, train_df, 
        eval_model_list, model_parameter_list, horizon, granularity)
    performance_df <- compute_performance_metrics_split(eval_forecast_df_list, eval_df)
    return(performance_df)
}

generate_cutoffs_date <- function(df, horizon, initial, period, granularity) {
    units <- paste0(granularity, "s")
    plugin_print(paste0("Cross-validation initial train set at ", initial, " ", units))
    plugin_print(paste0("Cross-validation cutoff period set at ", period, " ", units))
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
    plugin_print(paste("Making", length(result), "forecasts with cutoffs between", 
        result[length(result)], "and", result[1]))
    cutoffs <- rev(result)
    return(cutoffs)
}

compute_performance_metrics_crossval <- function(crossval_df_list, rolling_window = 1.0) {
    perf_df_list <- list()
    for(model_name in names(crossval_df_list)){
        tmp_df <- crossval_df_list[[model_name]]
        tmp_df[["horizon"]] <- tmp_df$ds - tmp_df$cutoff
        tmp_df <- tmp_df[order(tmp_df$horizon),]
        # Window size
        w <- as.integer(rolling_window * nrow(tmp_df))
        w <- max(w, 1)
        w <- min(w, nrow(tmp_df))
        cols <- c('horizon')
        for(metric in c("ME", "RMSE", "MAE", "MPE", "MAPE")) {
            tmp_df[[metric]] <- get(metric)(tmp_df, w)
            cols <- c(cols, metric)
        }
        tmp_df <- tmp_df[cols]
        tmp_df <- stats::na.omit(tmp_df)
        if(nrow(tmp_df) > 0) {
            perf_df_list[[model_name]] <- tmp_df
            perf_df_list[[model_name]]$model <- model_name
        }
    }
    performance_df <- do.call("rbind", perf_df_list) %>%
        select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE"))
    rownames(performance_df) <- NULL
    return(performance_df)
}

evaluate_models_crossval <- function(ts, df, model_list, model_parameter_list,
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
        history.df <- dplyr::filter(df, ds <= cutoff)
        if (nrow(history.df) < 2) {
            stop("[ERROR] Less than two datapoints before cutoff. Please increase initial window.")
        }
        history.ts <- head(ts, nrow(history.df))
        plugin_print(paste0("Training cross-validation step ", i ,"/", length(cutoffs), " for cutoff ", cutoffs[i], 
                            ", with ", length(history.ts), " rows in the train set"))
        df.predict <- head(dplyr::filter(df, ds > cutoff), horizon)
        eval_model_list <- train_forecasting_models(
            history.ts, history.df, model_parameter_list,
            refit = TRUE, refit_model_list = model_list,
            verbose = FALSE
        )
        forecast_df_list <- get_forecasts(history.ts, history.df,
            eval_model_list, model_parameter_list, horizon, granularity)
        for(model_name in names(forecast_df_list)){
            tmp_df <- dplyr::inner_join(df.predict, forecast_df_list[[model_name]], by = "ds") %>%
                select(ds, y, yhat, yhat_lower, yhat_upper)
            tmp_df$cutoff <- cutoff
            crossval_df_list[[model_name]] <- rbind(crossval_df_list[[model_name]] , tmp_df)
        }
    }
    performance_df <- compute_performance_metrics_crossval(crossval_df_list)
    return(performance_df)
}

evaluate_models <- function(ts, df, model_list, model_parameter_list, eval_strategy, 
                        horizon,  granularity, period = NULL, initial = NULL){
    if(eval_strategy == 'split'){
        performance_df <- evaluate_models_split(ts, df, model_list, model_parameter_list, 
            horizon, granularity)
    } 
    else if(eval_strategy == 'crossval') {
        performance_df <- evaluate_models_crossval(ts, df, model_list, model_parameter_list,
            horizon, granularity, period, initial) 
    }
    performance_df[["evaluation_horizon"]] <- as.integer(horizon)
    performance_df[["evaluation_period"]] <- ifelse(horizon==1, granularity, paste0(granularity,"s"))
    performance_df[["evaluation_strategy"]] <- eval_strategy
    return(performance_df)
}