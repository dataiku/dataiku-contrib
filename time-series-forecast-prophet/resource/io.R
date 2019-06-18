library(dataiku)
library(jsonlite)
library(R.utils)

# Set number of digits to use when printing Sys.time.
# It is needed to store version name in the model folder at millisecond granularity.
op <- options(digits.secs = 3)

PrintPlugin <- function(message, verbose = TRUE, stop = FALSE) {
  # Makes it easier to identify custom logging messages from the plugin.
  if (verbose) {
    if (stop) {
      msg <- paste0(
        "###########################################################\n",
        "[PLUGIN ERROR] ", message, "\n",
        "###########################################################"
      )
      cat(msg)
      stop(message)
    } else {
      msg <- paste0("[PLUGIN LOG] ", message)
      message(msg)
    }
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

  if (length(param) > 1) { # if the parameter has multiple values
    if (!is.null(names(param))) { # if parameter is a MAP
      output <- list()
      for(n in names(param)) {
        output[[n]] <- InferType(param[[n]])
      }
    } else { # if parameter is a COLUMNS list
        output <- param
    }
  } else if (length(param) == 0) { # if MAP parameter is empty
    output <- list()
  } else {
    output <- InferType(param)
  }
  return(output)
}

GetPartitioningDimension <- function() {
  # Gets the partition dimension name from the flow variables for recipe destinations.
  # Stops if there is more than one partitioning dimension.
  #
  # Args:
  #   None
  #
  # Returns:
  #   Partitioning dimension name

  flowVariables <- fromJSON(Sys.getenv("DKUFLOW_VARIABLES")) # not available in notebooks
  partitionDimensionName <- c()
  for (v in names(flowVariables)){
      if (substr(v, 1, 8) == "DKU_DST_"){
          partitionDimensionName <- c(partitionDimensionName, substr(v, 9, nchar(v)))
      }
  }
  if (length(partitionDimensionName) > 1) {
    PrintPlugin("Output must be partitioned by only one discrete dimension", stop = TRUE)
  } else if ("date" %in% partitionDimensionName) {
    PrintPlugin("Date dimension is not supported, please use only one discrete dimension", stop = TRUE)
  } else if (length(partitionDimensionName) == 1){
    partitionDimensionName <- partitionDimensionName[1]
  } else {
    partitionDimensionName <- ''
  }
  return(partitionDimensionName)
}

CheckPartitioningSettings <- function(inputDatasetName) {
  # Checks that partitioning settings are correct in case it is activated.
  #
  # Args:
  #   inputDatasetName: name of the input Dataiku dataset.
  #
  # Returns:
  #   "OK"  when partitioning is activated with correct settings,
  #   "NOK" when settings are incorrect,
  #   "NP"  when no partitioning is used.

  inputIsPartitioned <- dkuListDatasetPartitions(inputDatasetName)[1] != 'NP'
  partitionDimensionName <- GetPartitioningDimension()
  partitioningIsActivated <- partitionDimensionName != ''
  if (inputIsPartitioned && partitioningIsActivated) {
    check <- 'OK'
  } else if (!inputIsPartitioned && !partitioningIsActivated) {
    check <- "NP"
  } else {
    check <- "NOK"
    PrintPlugin("Partitioning should be activated on all input and output", stop = TRUE)
  }
  return(check)
}

WriteDatasetWithPartitioningColumn <- function(df, outputDatasetName) {
  # Writes a data.frame to a Dataiku dataset with a column to store the partition identifier
  # in case partitioning is activated, else writes the dataset without changes.
  # Needed for filesystem partioning when the partition identifier is not in the data itself.
  # It is very useful to have the partition written in the data in order to
  # build charts on the whole dataset. Could be an option in the native dataiku API?
  #
  # Args:
  #   df: data.frame to write
  #   outputDatasetName: name of the output Dataiku dataset.
  #
  # Returns:
  #   Nothing, simply writes dataframe to dataset

  outputFullName <- dataiku:::dku__resolve_smart_name(outputDatasetName) # bug with naming from plugins on DSS 5.0.2
  outputId <- dataiku:::dku__ref_to_name(outputFullName)
  outputDatasetType <- dkuGetDatasetLocationInfo(outputId)[["locationInfoType"]]
  partitionDimensionName <- GetPartitioningDimension()
  partitioningIsActivated <- partitionDimensionName != ''
  if (partitioningIsActivated && outputDatasetType != 'SQL') { # Filesystem partitioning
    partitioningColumnName <- paste0("_dku_partition_", partitionDimensionName)
    # writes partition identifier to the dataframe as a new column
    df[[partitioningColumnName]] <- dkuFlowVariable(paste0("DKU_DST_", partitionDimensionName))
    df <- df %>%
      select(partitioningColumnName, everything())
  }
  dkuWriteDataset(df, outputDatasetName)
}

GetFolderPathWithPartitioning <- function(folderName) {
  # Gets path to the folder partition if partitioning is activated, else path to the whole folder.
  # This is needed to write to a specific folder partition, as there are no dataiku R method
  # to get the *current* folder partition path with respect to a Build job.
  # Ideally this could be handled natively in our R API similarly to dkuWriteDataset
  # which writes to the current partition if the partition argument is blank.
  #
  # Args:
  #   folderName: dataiku folder name. Has to be on a local filesystem (enforced at recipe creation).
  #
  # Returns:
  #   Folder path with or without partitioning

  outputFolderIsPartitioned <- dkuManagedFolderDirectoryBasedPartitioning(folderName)
  partitionDimensionName <- GetPartitioningDimension()
  partitioningIsActivated <- partitionDimensionName != ''
  if (partitioningIsActivated && outputFolderIsPartitioned) {
    filePath <- file.path(
      dkuManagedFolderPath(folderName),
      dkuManagedFolderPartitionFolder(folderName,
        partition = dkuFlowVariable(paste0("DKU_DST_", partitionDimensionName)))
    )
    filePath <- normalizePath(gsub("//","/",filePath))
  } else if (partitioningIsActivated && ! outputFolderIsPartitioned) {
    PrintPlugin("Partitioning should be activated on output folder", stop = TRUE)
  } else {
    filePath <- dkuManagedFolderPath(folderName)
  }
  return(filePath)
}

SaveForecastingObjects <- function(folderName, versionName, ...) {
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

  folderPath <- GetFolderPathWithPartitioning(folderName)
  # create standard directory structure
  versionPath <- file.path(folderPath, "versions", versionName)
  dir.create(versionPath, recursive = TRUE)
  save(...,  file = file.path(versionPath , "models.RData"))
}

LoadForecastingObjects <- function(folderName, versionName = NULL) {
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

  folderPath <- GetFolderPathWithPartitioning(folderName)
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
  if (length(rdataPathList) == 0) {
    PrintPlugin("No Rdata files found in the model folder", stop = TRUE)
  }
  for (rdataPath in rdataPathList) {
    load(rdataPath, envir = .GlobalEnv)
  }
}
