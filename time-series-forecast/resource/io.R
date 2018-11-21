# Various utility functions used across the plugin for non-specific time series stuff

library(dataiku)
library(jsonlite)
library(R.utils)

plugin_print <- function(message, verbose = TRUE){
    if(verbose) message(paste("[PLUGIN_LOG]", message))
}

clean_plugin_config <- function(param){
    #' Infers types for a named list
    #'
    #' @description This returns a list with inferred types. 
    #' First it checks for numeric type, then boolean, and finally defaults to character
    if(is.list(param)){
        cleaned_list <- list()
        for(name in names(param)){
            if(!is.na(as.numeric(param[[name]]))){
                cleaned_list[[name]] <- as.numeric(param[[name]])
            } 
            else{
                if(!is.na(as.logical(param[[name]]))){
                    cleaned_list[[name]] <- as.logical(param[[name]])
                } 
                else{
                    cleaned_list[[name]] <- as.character(param[[name]])
                }
            }
        }
        return(cleaned_list)
    } 
    else {
        return(param)
    }
}

check_partitioning_settings <- function(input_dataset_name, partitioning_activated, partition_dimension_name){
    check <- 'NOK' # can be OK, NOK, or NP (not partitioned)
    error_msg <- ''
    input_is_partitioned <- dkuListDatasetPartitions(input_dataset_name)[1] !='NP' 
    if(!partitioning_activated && !input_is_partitioned){
        check <- "NP"
    } 
    else if (!partitioning_activated && input_is_partitioned){
        error_msg <- "Partitioning should activated in the recipe settings as input is partitioned"
    } 
    else { # check partition_dimension_name recipe settings if partitioning activated
        flow_variables_available <- Sys.getenv("DKU_CALL_ORIGIN") != 'notebook'
        if(flow_variables_available){
            flow_variables <- fromJSON(Sys.getenv("DKUFLOW_VARIABLES"))
            output_dimension_is_valid <- paste0("DKU_DST_", partition_dimension_name) %in% names(flow_variables)
            if(input_is_partitioned){ 
                if(is.null(partition_dimension_name) || partition_dimension_name == ''){
                    error_msg <- "Partitioning dimension name is required"
                } 
                else if(!output_dimension_is_valid) {
                    error_msg <- paste0("Dimension name '", partition_dimension_name,"' is invalid or output is not partitioned")
                } 
                else {
                    check <- "OK"
                }
            } else {
                if(!is.null(partition_dimension_name) && partition_dimension_name != '') {
                    error_msg <- "Partitioning dimension name should be left blank if input dataset is not partitioned"
                } 
                else if(output_dimension_is_valid) {
                    error_msg <- "All input and output should be partitioned"
                } 
                else {
                    check <- "NP"
                }
            }
        } 
        else {
            check <- ifelse(input_is_partitioned, "OK", "NP")
        }
    }
    
    plugin_print(paste0("Partitioning check returned ", check))
    if(check=='NOK') {
        stop(paste0("[ERROR] ", error_msg))
    } 
    else {
        return(check)
    }
}

write_dataset_with_partitioning_column <- function(df, output_dataset_name, partition_dimension_name, check_partitioning){
    output_fullName <- dataiku:::dku__resolve_smart_name(output_dataset_name) # bug with naming from plugins on DSS 5.0.2
    output_id <- dataiku:::dku__ref_to_name(output_fullName)
    output_dataset_type <- dkuGetDatasetLocationInfo(output_id)[["locationInfoType"]]
    if(check_partition == 'OK' && output_dataset_type != 'SQL') {
        plugin_print("Writing partition value as new column")
        partitioning_column_name <- paste0("_dku_partition_", partition_dimension_name)
        df[[partitioning_column_name]] <- dkuFlowVariable(paste0("DKU_DST_", partition_dimension_name))
        df <- df %>% select(partitioning_column_name, everything())
    }
    dkuWriteDataset(df, output_dataset_name)
}

get_folder_path_with_partitioning <- function(folder_name, partition_dimension_name, check_partitioning) {
    is_output_folder_partitioned <- dkuManagedFolderDirectoryBasedPartitioning(folder_name)
    if(check_partition == 'OK' && is_output_folder_partitioned){
        file_path <- file.path(
            dkuManagedFolderPath(folder_name),
            dkuManagedFolderPartitionFolder(folder_name, 
                partition = dkuFlowVariable(paste0("DKU_DST_", partition_dimension_name)))
        )
        file_path <- normalizePath(gsub("//","/",file_path))
    } 
    else if(check_partition == 'OK' && ! is_output_folder_partitioned){
        stop("[ERROR] Partitioning should be activated on all input and output")
    } 
    else {
        file_path <- dkuManagedFolderPath(folder_name)
    }
    return(file_path)
}

save_forecasting_objects <- function(folder_name, partition_dimension_name, version_name, ...) {
    folder_path <- get_folder_path_with_partitioning(folder_name, partition_dimension_name)
    # create standard directory structure
    version_path <- file.path(folder_path, "versions", version_name)
    dir.create(version_path, recursive = TRUE)
    save(...,  file = file.path(version_path , "models.RData"))
}

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