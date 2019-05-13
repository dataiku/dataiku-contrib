source(file.path(dkuCustomRecipeResource(), "train.R"))
source(file.path(dkuCustomRecipeResource(), "predict.R"))

ComputeErrorMetricsSplit <- function(forecastDfList, historyDf) {
  # Computes error metrics from the forecast and history data.frames on the same dates.
  # Utility function used inside the EvaluateModelsSplit function.
  #
  # Args:
  #   forecastDfList: named list of forecasts dataframes
  #                   (result of a call to the GetForecasts function).
  #   historyDf: data.frame of historical values.
  #
  # Returns:
  #   Data.frame with the evaluation of all models' split errors

  errorDfList <- list()
  for(modelName in names(forecastDfList)) {
    forecastDf <- forecastDfList[[modelName]]
    errorDfList[[modelName]] <- as.data.frame(
      forecast::accuracy(f = forecastDf$yhat, x = historyDf$y))
    errorDfList[[modelName]]$model <- modelName
  }
  errorDf <- do.call("rbind", errorDfList)
  rownames(errorDf) <- NULL
  return(errorDf)
}

## Functions copied from the prophet package
globalVariables(c("ds", "y", "cap", "yhat", "yhat_lower", "yhat_upper"))

rolling_mean <- function(x, w) {
  s <- cumsum(c(0, x))
  prefix <- rep(NA, w - 1)
  return(c(prefix, (s[(w + 1):length(s)] - s[1:(length(s) - w)]) / w))
}

mse <- function(df, w) {
  se <- (df$y - df$yhat) ** 2
  return(rolling_mean(se, w))
}

rmse <- function(df, w) {
  return(sqrt(mse(df, w)))
}

mae <- function(df, w) {
  ae <- abs(df$y - df$yhat)
  return(rolling_mean(ae, w))
}

mape <- function(df, w) {
  ape <- abs((df$y - df$yhat) / df$y)
  return(rolling_mean(ape, w))
}

EvaluateModelsSplit <- function(ts, df, xreg = NULL, modelList, modelParameterList, horizon, granularity) {
  # Evaluates forecast models on a time series according to the split strategy.
  #
  # Args:
  #   ts: input time series of R ts or msts class.
  #   df: input data frame following the Prophet format
  #       ("ds" column for time, "y" for series).
  #   xreg: matrix of external regressors (optional)
  #   modelList: named list of models (output of a call to the TrainForecastingModels function).
  #   modelParameterList: named list of model parameters set in the "Train and Evaluate" recipe UI.
  #   horizon: number of periods to evaluate the models.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #
  # Returns:
  #   Data.frame with the evaluation of all models' split errors

  trainRows <- length(ts) - horizon
  trainTs <- head(ts, trainRows)
  evalTs <- tail(ts, horizon)
  trainDf <- head(df, trainRows)
  evalDf <- tail(df, horizon)
  trainXreg <- head(xreg, trainRows) # NULL if xreg is NULL
  evalXreg <- tail(xreg, horizon) # NULL if xreg is NULL
  evalModelList <- TrainForecastingModels(
    trainTs, trainDf, trainXreg, modelParameterList,
    refit = TRUE, refitModelList = modelList,
    verbose = FALSE
  )
  evalForecastDfList <- GetForecasts(trainTs, trainDf, evalXreg,
    evalModelList, modelParameterList, horizon, granularity)
  errorDf <- ComputeErrorMetricsSplit(evalForecastDfList, evalDf)
  return(errorDf)
}

GenerateCutoffDatesCrossval <- function(df, horizon, granularity, initial, period) {
  # Generates list of cutoff dates for the cross-validation evaluation strategy.
  # It is required to get rolling time series splits across time.
  # Utility function used inside the EvaluateModelsCrossval function.
  # The last cutoff date is the last-horizon-th value,
  # then a cutoff is made every cutoff period,
  # until it reaches the initial training set size.
  #
  # Args:
  #   df: input data frame following the Prophet format
  #       ("ds" column for time, "y" for series).
  #   horizon: number of periods to evaluate the models.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #   initial: number of periods in the initial train set.
  #   period: number of periods between cutoff dates.
  #
  # Returns:
  #   List of cutoff dates

  units <- paste0(granularity, "s")
  if (granularity %in% c("hour", "day", "week")) {
    # Taken from the prophet:::generate_cutoffs function
    horizon.dt <- as.difftime(horizon, units = units)
    initial.dt <- as.difftime(initial, units = units)
    period.dt <- as.difftime(period, units = units)
    cutoff <- max(df$ds) - horizon.dt # last cutoff point
    result <- c(cutoff)
    while (result[length(result)] >= min(df$ds) + initial.dt) {
      cutoff <- cutoff - period.dt # moves back in time by horizon
      if (!any((df$ds > cutoff) & (df$ds <= cutoff + horizon.dt))) {
        closest.date <- max(df$ds[df$ds <= cutoff])
        cutoff <- closest.date - horizon.dt
      }
      result <- c(result, cutoff)
    }
  } else {
    # difftime does not work with monthly/quarterly/yearly data so this is another implementation.
    # It assumes that input data is sorted and resampled at the given granularity.
    # This is OK provided the data has been cleaned with the Clean plugin recipe beforehand.
    dates <- sort(df$ds)
    cutoffIndex <- length(dates) - horizon
    cutoff <- dates[cutoffIndex]
    result <- c(cutoff)
    while(result[length(result)] >= dates[initial+1]) {
      cutoffIndex <- cutoffIndex - period
      cutoff <- dates[cutoffIndex] # last cutoff point
      if (cutoffIndex <= 1) {
        result <- c(result, NA)
        break # break while loop when reaching the start of the time series
      }
      if (!any((dates > cutoff) & (dates <= dates[cutoffIndex + horizon]))) {
        cutoff <- dates[cutoffIndex - horizon] # moves back in time by horizon
      }
      result <- c(result, cutoff)
    }
  }
  result <- utils::head(result, -1) # the last element of the while loop needs to be removed
  PrintPlugin(paste("Making", length(result), "forecasts with cutoffs between",
    result[length(result)], "and", result[1]))
  cutoffs <- rev(result) # restores the natural order of time since the while loop is backwards
  return(cutoffs)
}

ComputeErrorMetricsCrossval <- function(crossvalDfList, rollingWindow = 1.0) {
  # Computes error metrics from crossvalidation dataframes using rolling windows.
  # Utility function used inside the EvaluateModelsCrossval function.
  #
  # Args:
  #   crossvalDfList: named list of crossvalidation dataframes according to
  #                   the prophet::cross_validation output format.
  #   rollingWindow: proportion of data to use in each rolling window for
  #                  computing the metrics. Should be in [0, 1].
  #                  1.0 (default) will output a single value for the entire horizon.
  #
  # Returns:
  #   Data.frame with the evaluation of all models' cross-validation errors

  errorDfList <- list()
  for(modelName in names(crossvalDfList)) {
    tmpDf <- crossvalDfList[[modelName]]
    tmpDf[["horizon"]] <- tmpDf$ds - tmpDf$cutoff
    tmpDf <- tmpDf[order(tmpDf$horizon),]
    # Window size
    w <- as.integer(rollingWindow * nrow(tmpDf))
    w <- max(w, 1)
    w <- min(w, nrow(tmpDf))
    # # ME and MPE are not implemented in Prophet
    tmpDf[["ME"]] <- rolling_mean(tmpDf$y - tmpDf$yhat, w)
    tmpDf[["MPE"]] <- rolling_mean((tmpDf$y - tmpDf$yhat)/tmpDf$y, w)
    # # Other error metrics have built-in prophet implementations
    tmpDf[["MAE"]] <- mae(tmpDf, w)
    tmpDf[["MAPE"]] <- mape(tmpDf, w)
   tmpDf[["RMSE"]] <- rmse(tmpDf, w)
    tmpDf <- stats::na.omit(tmpDf)
    if (nrow(tmpDf) > 0) {
      errorDfList[[modelName]] <- tmpDf
      errorDfList[[modelName]]$model <- modelName
    }
  }
  errorDf <- do.call("rbind", errorDfList)
  rownames(errorDf) <- NULL
  return(errorDf)
}

EvaluateModelsCrossval <- function(ts, df, xreg = NULL, modelList, modelParameterList,
  horizon, granularity, initial, period) {
  # Evaluates forecast models on a time series according to the cross-validation strategy.
  #
  # Args:
  #   ts: input time series of R ts or msts class.
  #   df: input data frame following the Prophet format
  #       ("ds" column for time, "y" for series).
  #   xreg: matrix of external regressors (optional)
  #   modelList: named list of models (output of a call to the TrainForecastingModels function).
  #   modelParameterList: named list of model parameters set in the "Train and Evaluate" recipe UI.
  #   horizon: number of periods to evaluate the models.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #   initial: number of periods in the initial train set.
  #   period: number of periods between cutoff dates.
  #
  # Returns:
  #   Data.frame with the evaluation of all models' cross-validation errors

  cutoffs <- GenerateCutoffDatesCrossval(df, horizon, granularity, initial, period)
  crossvalDfList <- list()
  for(modelName in names(modelList)) {
    crossvalDfList[[modelName]] <- data.frame()
  }
  # compute forecasts for all cutoffs
  for(i in 1:length(cutoffs)) {
    cutoff <- cutoffs[i]
    trainDf <- dplyr::filter(df, ds <= cutoff)
    trainRows <- nrow(trainDf)
    if (trainRows < 2) {
      PrintPlugin("Less than two datapoints before cutoff. Please increase initial training.", stop = TRUE)
    }
    trainTs <- head(ts, trainRows)
    trainXreg <- head(xreg, trainRows) # NULL if xreg is NULL
    evalDf <- head(dplyr::filter(df, ds > cutoff), horizon)
    evalXreg <- NULL
    if (!is.null(xreg)) {
      evalXreg <- as.matrix(xreg[(trainRows + 1):(trainRows + horizon),])
      colnames(evalXreg) <- colnames(xreg)
    }
    PrintPlugin(paste0("Crossval split ", i ,"/", length(cutoffs), " at cutoff ", cutoffs[i],
      " with ", length(trainTs), " training rows"))
    evalModelList <- TrainForecastingModels(
      trainTs, trainDf, trainXreg, modelParameterList,
      refit = TRUE, refitModelList = modelList,
      verbose = FALSE
    )
    forecastDfList <- GetForecasts(trainTs, trainDf, evalXreg,
      evalModelList, modelParameterList, horizon, granularity)
    for(modelName in names(forecastDfList)) {
      tmpDf <- dplyr::inner_join(evalDf, forecastDfList[[modelName]], by = "ds") %>%
        select(ds, y, yhat, yhat_lower, yhat_upper)
      tmpDf$cutoff <- cutoff
      crossvalDfList[[modelName]] <- rbind(crossvalDfList[[modelName]] , tmpDf)
    }
  }
  errorDf <- ComputeErrorMetricsCrossval(crossvalDfList)
  return(errorDf)
}

EvaluateModels <- function(ts, df, xreg = NULL, modelList, modelParameterList, evalStrategy,
  horizon, granularity, initial = NULL, period = NULL) {
  # Evaluates multiple forecast models on a time series according to
  # the specified evaluation strategy.
  #
  # Args:
  #   ts: input time series of R ts or msts class.
  #   df: input data frame following the Prophet format
  #       ("ds" column for time, "y" for series).
  #   xreg: matrix of external regressors (optional)
  #   modelList: named list of models (output of a call to the TrainForecastingModels function).
  #   modelParameterList: named list of model parameters set in the "Train and Evaluate" recipe UI.
  #   evalStrategy: character string describing which evaluation strategy to use
  #                 (one of "split", "crossval").
  #   horizon: number of periods to evaluate the models.
  #   granularity: character string (one of "year", "quarter", "month", "week", "day", "hour").
  #   initial: number of periods in the initial train set.
  #   period: number of periods between cutoff dates.
  #
  # Returns:
  #   Data.frame with the evaluation of all models' errors

  if (evalStrategy == 'split') {
    errorDf <- EvaluateModelsSplit(ts, df, xreg, modelList, modelParameterList,
      horizon, granularity)
  } else if (evalStrategy == 'crossval') {
    errorDf <- EvaluateModelsCrossval(ts, df, xreg, modelList, modelParameterList,
      horizon, granularity, initial, period)
  }
  errorDf <- errorDf %>%
    select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE")) %>%
    rename(mean_error = ME, root_mean_square_error = RMSE, mean_absolute_error = MAE,
      mean_percentage_error = MPE, mean_absolute_percentage_error = MAPE) %>%
    mutate(model = recode(model, !!!MODEL_UI_NAME_LIST)) %>%
    mutate_all(funs(ifelse(is.infinite(.), NA, .)))
  errorDf[["evaluation_horizon"]] <- as.integer(horizon)
  errorDf[["evaluation_period"]] <- ifelse(horizon==1, granularity, paste0(granularity,"s"))
  errorDf[["evaluation_strategy"]] <- evalStrategy
  return(errorDf)
}
