# Various utility functions used across the plugin for non-specific time series stuff

library(dataiku)
library(jsonlite)
library(R.utils)

PrintPlugin <- function(message, verbose = TRUE) {
  if (verbose) {
    message(paste("[PLUGIN_LOG]", message))
  }
}

InferType <- function(x) {
  if (!is.na(suppressWarnings(as.numeric(x)))) {
    xInferred <- as.numeric(x)
  } else if (!is.na(suppressWarnings(as.logical(x)))) {
    xInferred <- as.logical(x)
  } else {
     xInferred <- as.character(x)
  }
  names(xInferred) <- names(x)
  return(xInferred)
}

CleanPluginParam <- function(param) {
  if (length(param) > 1) {
    output <- list()
    for(n in names(param)) {
      output[[n]] <- InferType(param[[n]])
    }
  } else if (length(param) == 0) {
    output <- list()
  } else {
    output <- InferType(param)
  }
  return(output)
}

CheckPartitioningSettings <- function(inputDatasetName, partitioningActivated, partitionDimensionName) {
  check <- 'NOK' # can be OK, NOK, or NP (not partitioned)
  errorMsg <- ''
  inputIsPartitioned <- dkuListDatasetPartitions(inputDatasetName)[1] !='NP' 
  if (!partitioningActivated && !inputIsPartitioned) {
    check <- "NP"
  } else if (!partitioningActivated && inputIsPartitioned) {
    errorMsg <- "Partitioning should activated in the recipe settings as input is partitioned"
  } else { # check partitionDimensionName recipe settings if partitioning activated
    flowVariablesAvailable <- Sys.getenv("DKU_CALL_ORIGIN") != 'notebook'
    if (flowVariablesAvailable) {
      flowVariables <- fromJSON(Sys.getenv("DKUFLOW_VARIABLES"))
      outputDimensionIsValid <- paste0("DKU_DST_", partitionDimensionName) %in% names(flowVariables)
      if (inputIsPartitioned) { 
        if (is.null(partitionDimensionName) || partitionDimensionName == '') {
          errorMsg <- "Partitioning dimension name is required"
        } else if (!outputDimensionIsValid) {
          errorMsg <- paste0("Dimension name '", partitionDimensionName,"' is invalid or output is not partitioned")
        } else {
          check <- "OK"
        }
      } else {
        if (!is.null(partitionDimensionName) && partitionDimensionName != '') {
          errorMsg <- "Partitioning dimension name should be left blank if input dataset is not partitioned"
        } else if (outputDimensionIsValid) {
          errorMsg <- "All input and output should be partitioned"
        } else {
          check <- "NP"
        }
      }
    } else {
      check <- ifelse(inputIsPartitioned, "OK", "NP")
    }
  }
  
  PrintPlugin(paste0("Partitioning check returned ", check))
  if (check=='NOK') {
    stop(paste0("[ERROR] ", errorMsg))
  } else {
    return(check)
  }
}

WriteDatasetWithPartitioningColumn <- function(df, outputDatasetName, partitionDimensionName, checkPartitioning) {
  outputFullName <- dataiku:::dku__resolve_smart_name(outputDatasetName) # bug with naming from plugins on DSS 5.0.2
  outputId <- dataiku:::dku__ref_to_name(outputFullName)
  outputDatasetType <- dkuGetDatasetLocationInfo(outputId)[["locationInfoType"]]
  if (checkPartition == 'OK' && outputDatasetType != 'SQL') {
    PrintPlugin("Writing partition value as new column")
    partitioningColumnName <- paste0("_dku_partition_", partitionDimensionName)
    df[[partitioningColumnName]] <- dkuFlowVariable(paste0("DKU_DST_", partitionDimensionName))
    df <- df %>% select(partitioningColumnName, everything())
  }
  dkuWriteDataset(df, outputDatasetName)
}

GetFolderPathWithPartitioning <- function(folderName, partitionDimensionName, checkPartitioning) {
  isOutputFolderPartitioned <- dkuManagedFolderDirectoryBasedPartitioning(folderName)
  if (checkPartition == 'OK' && isOutputFolderPartitioned) {
    filePath <- file.path(
      dkuManagedFolderPath(folderName),
      dkuManagedFolderPartitionFolder(folderName, 
        partition = dkuFlowVariable(paste0("DKU_DST_", partitionDimensionName)))
    )
    filePath <- normalizePath(gsub("//","/",filePath))
  } else if (checkPartition == 'OK' && ! isOutputFolderPartitioned) {
    stop("[ERROR] Partitioning should be activated on all input and output")
  } else {
    filePath <- dkuManagedFolderPath(folderName)
  }
  return(filePath)
}

SaveForecastingObjects <- function(folderName, partitionDimensionName, versionName, ...) {
  folderPath <- GetFolderPathWithPartitioning(folderName, partitionDimensionName)
  # create standard directory structure
  versionPath <- file.path(folderPath, "versions", versionName)
  dir.create(versionPath, recursive = TRUE)
  save(...,  file = file.path(versionPath , "models.RData"))
}

LoadForecastingObjects <- function(modelFolderName, partitionDimensionName, envir = .GlobalEnv) {
  folderPath <- GetFolderPathWithPartitioning(modelFolderName, PARTITION_DIMENSION_NAME)
  lastVersionPath <- max(list.dirs(file.path(folderPath, "versions"), recursive = FALSE))
  PrintPlugin(paste0("Loading forecasting objects from path ", lastVersionPath))
  rdataPathList <- list.files(
    path = lastVersionPath,
    pattern = "*.RData",
    full.names = TRUE,
    recursive = TRUE
  )
  for(rdataPath in rdataPathList) {
    load(rdataPath, envir = envir)
  }
}