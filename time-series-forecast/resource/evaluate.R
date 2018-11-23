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

ComputePerformanceMetricsSplit <- function(forecastDfList, historyDF) {
  dfList <- list()
  for(modelName in names(forecastDfList)) {
    forecastDF <- forecastDfList[[modelName]]
    dfList[[modelName]] <- as.data.frame(forecast::accuracy(f = forecastDF$yhat, x = historyDF$y))
    dfList[[modelName]]$model <- modelName
  }
  performanceDF <- do.call("rbind", dfList) %>%
    select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE"))
  rownames(performanceDF) <- NULL
  return(performanceDF)
}

EvaluateModelsSplit <- function(ts, df, modelList, modelParameterList, horizon, granularity) {
  trainTS <- head(ts, length(ts) - horizon)
  evalTS <- tail(ts, horizon)
  trainDF <- head(df, nrow(df) - horizon)
  evalDF <- tail(df, horizon)
  evalModelList <- TrainForecastingModels(
    trainTS, trainDF, modelParameterList,
    refit = TRUE, refitModelList = modelList,
    verbose = FALSE
  )
  evalForecastDfList <- GetForecasts(trainTS, trainDF, 
    evalModelList, modelParameterList, horizon, granularity)
  performanceDF <- ComputePerformanceMetricsSplit(evalForecastDfList, evalDF)
  return(performanceDF)
}

GenerateCutoffsDate <- function(df, horizon, initial, period, granularity) {
  units <- paste0(granularity, "s")
  PrintPlugin(paste0("Cross-validation initial train set at ", initial, " ", units))
  PrintPlugin(paste0("Cross-validation cutoff period set at ", period, " ", units))
  if (granularity %in% c("hour", "day", "week")) {
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
  } else {
    # difftime does not work with monthly, quarterly or yearly data
    # assumes that input data is regularly sampled (checks are implemented earlier)
    dates <- sort(df$ds)
    cutoffIndex <- length(dates) - horizon
    cutoff <- dates[cutoffIndex]
    result <- c(cutoff)
    while(result[length(result)] >= dates[initial+1]) {
      cutoffIndex <- cutoffIndex - period
      cutoff <- dates[cutoffIndex]
      if (cutoffIndex <= 1) {
        result <- c(result, NA)
        break
      }
      if (!any((dates > cutoff) & (dates <= dates[cutoffIndex + horizon]))) {
        cutoff <- dates[cutoffIndex - horizon] 
      }
      result <- c(result, cutoff)
    }
  }
  result <- utils::head(result, -1)
  PrintPlugin(paste("Making", length(result), "forecasts with cutoffs between", 
    result[length(result)], "and", result[1]))
  cutoffs <- rev(result)
  return(cutoffs)
}

ComputePerformanceMetricsCrossval <- function(crossvalDfList, rollingWindow = 1.0) {
  perfDfList <- list()
  for(modelName in names(crossvalDfList)) {
    tmpDf <- crossvalDfList[[modelName]]
    tmpDf[["horizon"]] <- tmpDf$ds - tmpDf$cutoff
    tmpDf <- tmpDf[order(tmpDf$horizon),]
    # Window size
    w <- as.integer(rollingWindow * nrow(tmpDf))
    w <- max(w, 1)
    w <- min(w, nrow(tmpDf))
    cols <- c('horizon')
    for(metric in c("ME", "RMSE", "MAE", "MPE", "MAPE")) {
      tmpDf[[metric]] <- get(metric)(tmpDf, w)
      cols <- c(cols, metric)
    }
    tmpDf <- tmpDf[cols]
    tmpDf <- stats::na.omit(tmpDf)
    if (nrow(tmpDf) > 0) {
      perfDfList[[modelName]] <- tmpDf
      perfDfList[[modelName]]$model <- modelName
    }
  }
  performanceDF <- do.call("rbind", perfDfList) %>%
    select_(.dots = c("model", "ME", "RMSE", "MAE", "MPE", "MAPE"))
  rownames(performanceDF) <- NULL
  return(performanceDF)
}

EvaluateModelsCrossval <- function(ts, df, modelList, modelParameterList,
                    horizon, granularity, period = NULL, initial = NULL) {
  if (is.null(period)) {
    period <- 0.5 * horizon
  }
  if (is.null(initial)) {
    initial <- 10 * horizon
  }
  cutoffs <- GenerateCutoffsDate(df, horizon, initial, period, granularity)
  crossvalDfList <- list()
  for(modelName in names(modelList)) {
    crossvalDfList[[modelName]] <- data.frame()
  }
  # compute forecasts for all cutoffs
  for(i in 1:length(cutoffs)) {
    cutoff <- cutoffs[i]
    history.df <- dplyr::filter(df, ds <= cutoff)
    if (nrow(history.df) < 2) {
      stop("[ERROR] Less than two datapoints before cutoff. Please increase initial window.")
    }
    history.ts <- head(ts, nrow(history.df))
    PrintPlugin(paste0("Training cross-validation step ", i ,"/", length(cutoffs), " for cutoff ", cutoffs[i], 
              ", with ", length(history.ts), " rows in the train set"))
    df.predict <- head(dplyr::filter(df, ds > cutoff), horizon)
    evalModelList <- TrainForecastingModels(
      history.ts, history.df, modelParameterList,
      refit = TRUE, refitModelList = modelList,
      verbose = FALSE
    )
    forecastDfList <- GetForecasts(history.ts, history.df,
      evalModelList, modelParameterList, horizon, granularity)
    for(modelName in names(forecastDfList)) {
      tmpDf <- dplyr::inner_join(df.predict, forecastDfList[[modelName]], by = "ds") %>%
        select(ds, y, yhat, yhat_lower, yhat_upper)
      tmpDf$cutoff <- cutoff
      crossvalDfList[[modelName]] <- rbind(crossvalDfList[[modelName]] , tmpDf)
    }
  }
  performanceDF <- ComputePerformanceMetricsCrossval(crossvalDfList)
  return(performanceDF)
}

EvaluateModels <- function(ts, df, modelList, modelParameterList, evalStrategy, 
            horizon,  granularity, period = NULL, initial = NULL) {
  if (evalStrategy == 'split') {
    performanceDF <- EvaluateModelsSplit(ts, df, modelList, modelParameterList, 
      horizon, granularity)
  } else if (evalStrategy == 'crossval') {
    performanceDF <- EvaluateModelsCrossval(ts, df, modelList, modelParameterList,
      horizon, granularity, period, initial) 
  }
  performanceDF[["evaluation_horizon"]] <- as.integer(horizon)
  performanceDF[["evaluation_period"]] <- ifelse(horizon==1, granularity, paste0(granularity,"s"))
  performanceDF[["evaluation_strategy"]] <- evalStrategy
  return(performanceDF)
}