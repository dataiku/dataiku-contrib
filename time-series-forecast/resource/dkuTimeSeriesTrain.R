# Functions used for the train and evaluate recipe

library(forecast)
library(prophet)
library(R.utils)
library(dataiku)
library(dplyr)
library(tibble)

source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))

naive_model_wrapper <- function(y, method="simple", model= NULL, ...) {
    #' Wrap naive models from the forecast package in a simpler standard way
    #'
    #' @description Depending on a simple "method" argument, this wrapper function switches to 
    #' different implementations of naive models in the forecast package. 
    
    model <- switch(method,
        simple = forecast::naive(y, ...),
        seasonal = forecast::snaive(y, ...),
        drift = forecast::rwf(y, drift=TRUE, ...)
    )
    
    return(model)
}


prophet_model_wrapper <- function(df, growth='linear', model=NULL, y=NULL, ...){
    if(is.null(model)){
        m <- prophet(df, growth=growth, ...)
    }
    else{
        cutoff <- max(df$ds)
        m2 <- prophet:::prophet_copy(model, cutoff)
        m <- fit.prophet(m2, df)
    }
   return(m)
}

train_forecasting_models <- function(ts, df, model_parameter_list, refit=FALSE, refit_model_list=NULL) {
    model_list <- list()
    for(model_name in names(model_parameter_list)){
        model_activated <- get(paste0(model_name,"_ACTIVATED"))
        if(model_activated){
            model_parameters <- model_parameter_list[[model_name]]
            if(refit && !is.null(refit_model_list)){
                model_parameters[["kwargs"]][["model"]] = refit_model_list[[model_name]]
            }
            if(model_name == "PROPHET_MODEL"){
                model_parameters[["kwargs"]][["df"]] <- df
            }
            plugin_print(paste0(model_name," training starting"))
            start_time = Sys.time()
            model_list[[model_name]] <- R.utils::doCall(
                .fcn = model_parameters[["model_function"]],
                y = ts,
                args = model_parameters[["kwargs"]],
                .ignoreUnusedArgs = TRUE
            )
            end_time = Sys.time()
            plugin_print(paste0(model_name," training completed after ", 
                                round(end_time - start_time, 1), " seconds"))
        }
    }
    return(model_list)
}


save_forecasting_objects <- function(folder_name, partition_dimension_name, version_name, ts, df, model_parameter_list, model_list) {
    #' Save R models and arbitrary objects to a filesystem folder in Rdata format with a defined structure
    #'
    #' @description First, it creates a structure inside the directory:
    #' - version/<timestamp in ms>/ for parameters and time series,
    #' - version/<timestamp in ms>/models for models.
    #' Then it saves the objects in Rdata format to the relevant directory.
    #' It handles versioning of models so all trained models are saved.
    
    folder_path <- get_folder_path_with_partitioning(folder_name, partition_dimension_name)
    
    # create standard directory structure
    version_path <- file.path(folder_path, "versions", version_name)
    dir.create(version_path, recursive = TRUE)
    
    save(ts, file = file.path(version_path , "ts.RData"))
    save(df, file = file.path(version_path , "df.RData"))
    save(model_parameter_list, file = file.path(version_path , "params.RData"))
    save(model_list, file = file.path(version_path , "models.RData"))
}