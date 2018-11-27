# Functions used across the plugin for Input/Output reading and writing

library(dataiku)
library(jsonlite)
library(R.utils)

PrintPlugin <- function(message, verbose = TRUE) {
  # Makes it easier to identify custom logging messages from the plugin.
  if (verbose) {
    message(paste("[PLUGIN_LOG]", message))
  }
}

InferType <- function(x) {
  # Infers the type of a character object and retains its name.
  #
  # Args:
  #   x: atomic character element.
  #
  # Returns:
  #   Object of inferred type with the same name as the input 

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
  # Inters the types of a plugin parameter object.
  # Required to pass objects from a plugin MAP parameter (where everything is character)
  # to R functions which sometime expect numeric or boolean objects.
  #
  # Args:
  #   param: plugin parameter (list element given by dkuCustomRecipeConfig)
  #
  # Returns:
  #   Parameter of inferred type

  if (length(param) > 1) { # if the parameter is MAP and non-empty
    output <- list()
    for(n in names(param)) {
      output[[n]] <- InferType(param[[n]])
    }
  } else if (length(param) == 0) { # if MAP parameter is empty
    output <- list()
  } else { 
    output <- InferType(param)
  }
  return(output)
}

CheckPartitioningSettings <- function(inputDatasetName, partitioningActivated, partitionDimensionName) {
  # Checks that partitioning settings are correct in case it is activated.
  #
  # Args:
  #   inputDatasetName: name of the input Dataiku dataset.
  #   partitioningActivated: boolean if partitioning is activated in the plugin UI.
  #   partitionDimensionName: partition dimension name specified in the plugin UI.
  #
  # Returns:
  #   "OK"  when partitioning is activated with correct settings,
  #   "NOK" when settings are incorrect,
  #   "NP"  when no partitioning is used.

  check <- 'NOK' # can be OK, NOK, or NP (not partitioned)
  errorMsg <- ''
  inputIsPartitioned <- dkuListDatasetPartitions(inputDatasetName)[1] !='NP' 
  if (!partitioningActivated && !inputIsPartitioned) {
    check <- "NP"
  } else if (!partitioningActivated && inputIsPartitioned) {
    errorMsg <- "Partitioning should activated in the recipe settings as input is partitioned"
  } else { 
    flowVariables <- fromJSON(Sys.getenv("DKUFLOW_VARIABLES"))
    outputDimensionIsValid <- paste0("DKU_DST_", partitionDimensionName) %in% names(flowVariables)
    if (inputIsPartitioned) {  # case when input dataset is partitioned
      if (is.null(partitionDimensionName) || partitionDimensionName == '') {
        errorMsg <- "Partitioning dimension name is required"
      } else if (!outputDimensionIsValid) {
        errorMsg <- paste0("Dimension name '", partitionDimensionName,
          "' is invalid or output is not partitioned")
      } else {
        check <- "OK"
      }
    } else { # case when input dataset is not partitioned
      if (!is.null(partitionDimensionName) && partitionDimensionName != '') {
        errorMsg <- "Partitioning dimension name should be left blank if input dataset is not partitioned"
      } else if (outputDimensionIsValid) {
        errorMsg <- "All input and output should be partitioned"
      } else {
        check <- "NP"
      }
    }
  }
  PrintPlugin(paste0("Partitioning check returned ", check))
  return(check)
  if (check == 'NOK') {
    stop(paste0("[ERROR] ", errorMsg))
  }
}

WriteDatasetWithPartitioningColumn <- function(df, outputDatasetName, partitionDimensionName, checkPartitioning) {
  # Writes a data.frame to a Dataiku dataset with a column to store the partition identifier
  # in case partitioning is activated, else writes the dataset without changes.
  # Needed for filesystem partioning when the partition identifier is not in the data itself. 
  # It is very useful to have the partition written in the data in order to 
  # build charts on the whole dataset. Could be an option in the native dataiku API?
  #
  # Args:
  #   df: data.frame to write
  #   outputDatasetName: name of the output Dataiku dataset.
  #   partitionDimensionName: partition dimension name specified in the plugin UI.
  #   checkPartitioning: output of a call to the CheckPartitioningSettings function.
  #
  # Returns:
  #   Nothing, simply writes dataframe to dataset

  outputFullName <- dataiku:::dku__resolve_smart_name(outputDatasetName) # bug with naming from plugins on DSS 5.0.2
  outputId <- dataiku:::dku__ref_to_name(outputFullName)
  outputDatasetType <- dkuGetDatasetLocationInfo(outputId)[["locationInfoType"]]
  if (checkPartitioning == 'OK' && outputDatasetType != 'SQL') { # Filesystem partitioning
    PrintPlugin("Writing partition value as new column")
    partitioningColumnName <- paste0("_dku_partition_", partitionDimensionName)
    # writes partition identifier to the dataframe as a new column
    df[[partitioningColumnName]] <- dkuFlowVariable(paste0("DKU_DST_", partitionDimensionName))
    df <- df %>% 
      select(partitioningColumnName, everything())
  }
  dkuWriteDataset(df, outputDatasetName)
  PrintPlugin(paste0("Wrote dataframe to dataset ", outputDatasetName))
}

GetFolderPathWithPartitioning <- function(folderName, partitionDimensionName, checkPartitioning) {
  # Gets path to the folder partition if partitioning is activated, else path to the whole folder. 
  # This is needed to write to a specific folder partition, as there are no dataiku R method
  # to get the *current* folder partition path with respect to a Build job.
  # Ideally this could be handled natively in our R API similarly to dkuWriteDataset
  # which writes to the current partition if the partition argument is blank.
  #
  # Args:
  #   folderName: dataiku folder name. Has to be on a local filesystem (enforced at recipe creation).
  #   partitionDimensionName: partition dimension name specified in the plugin UI.
  #   checkPartitioning: output of a call to the CheckPartitioningSettings function.
  #
  # Returns:
  #   Folder path with or without partitioning

  isOutputFolderPartitioned <- dkuManagedFolderDirectoryBasedPartitioning(folderName)
  if (checkPartitioning == 'OK' && isOutputFolderPartitioned) {
    filePath <- file.path(
      dkuManagedFolderPath(folderName),
      dkuManagedFolderPartitionFolder(folderName, 
        partition = dkuFlowVariable(paste0("DKU_DST_", partitionDimensionName)))
    )
    filePath <- normalizePath(gsub("//","/",filePath))
  } else if (checkPartitioning == 'OK' && ! isOutputFolderPartitioned) {
    stop("[ERROR] Partitioning should be activated on all input and output")
  } else {
    filePath <- dkuManagedFolderPath(folderName)
  }
  return(filePath)
}

SaveForecastingObjects <- function(folderName, partitionDimensionName, 
  checkPartitioning, versionName, ...) {
  # Saves forecasting objects to a single Rdata file in an output folder.
  # Creates a standard directory structure folderpath/(partitionid/)versions/models.RData.
  #
  # Args:
  #   folderName: dataiku folder name. Has to be on a local filesystem (enforced at recipe creation).
  #   partitionDimensionName: partition dimension name specified in the plugin UI.
  #   checkPartitioning: output of a call to the CheckPartitioningSettings function.
  #   versionName: identifier of the version of the forecasting objects
  #   ...: objects to save to the folder
  #
  # Returns:
  #   Nothing, simply writes RData file to folder

  folderPath <- GetFolderPathWithPartitioning(folderName, partitionDimensionName, checkPartitioning)
  # create standard directory structure
  versionPath <- file.path(folderPath, "versions", versionName)
  dir.create(versionPath, recursive = TRUE)
  save(...,  file = file.path(versionPath , "models.RData"))
}

LoadForecastingObjects <- function(folderName, partitionDimensionName, 
  checkPartitioning, versionName = NULL) {
  # Loads forecasting objects from the folder with saved forecasting objects
  # written by the SaveForecastingObjects function.
  #
  # Args:
  #   folderName: dataiku folder name. Has to be on a local filesystem (enforced at recipe creation).
  #   partitionDimensionName: partition dimension name specified in the plugin UI.
  #   checkPartitioning: output of a call to the CheckPartitioningSettings function.
  #   versionName: identifier of the version of the forecasting objects.
  #
  # Returns:
  #   Nothing, simply loads RData file from folder to the global R environment

  folderPath <- GetFolderPathWithPartitioning(folderName, PARTITION_DIMENSION_NAME, checkPartitioning)
  if (is.null(versionName)) {
    lastVersionPath <- max(list.dirs(file.path(folderPath, "versions"), recursive = FALSE))
  } else {
    lastVersionPath <- file.path(folderPath, "versions", versionName)
  }
  PrintPlugin(paste0("Loading forecasting objects from path ", lastVersionPath))
  rdataPathList <- list.files(
    path = lastVersionPath,
    pattern = "*.RData",
    full.names = TRUE,
    recursive = TRUE
  )
  for(rdataPath in rdataPathList) {
    load(rdataPath, envir = .GlobalEnv)
  }
}