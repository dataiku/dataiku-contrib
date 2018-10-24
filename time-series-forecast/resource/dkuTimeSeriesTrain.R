# Functions used for the train evaluate recipe

library(forecast)
library(prophet)
library(R.utils)
library(dataiku)
source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))

dkuNaive <- function(y, method, lambda, biasadj) {
    #' Wrap naive models from the forecast package in a simpler standard way
    #'
    #' @description Depending on a simple "method" argument, this wrapper function switches to 
    #' different implementations of naive models in the forecast package. 
    
    model <- switch(method,
        simple = forecast::naive(y, lambda = lambda, biasadj = biasadj),
        seasonal = forecast::snaive(y, lambda = lambda, biasadj = biasadj),
        drift = forecast::rwf(y, drift=TRUE, lambda = lambda, biasadj = biasadj)
    )
    
    return(model)
}


dkuProphet <- function(y, growth, floor=NULL, cap=NULL, ...){
    # we actually use the original df object from the global environment
    # this is faster than convert the ts object back to a dataframe
    
    df_prophet <- df
    names(df_prophet) <- c('ds','y')
    if(growth=='logistic'){
        df_prophet[['cap']] <- cap
        df_prophet[['floor']] <- floor
   }
   return(prophet(df_prophet, growth=growth, ...))
}

save_forecasting_objects <- function(folder_name, partition_dimension_name, version_name, ts, df, model_parameter_list, model_list) {
    #' Save R models and arbitrary objects to a filesystem folder in Rdata format with a defined structure
    #'
    #' @description First, it creates a structure inside the directory:
    #' - version/<timestamp in ms>/ for parameters and time series,
    #' - version/<timestamp in ms>/models for models.
    #' Then it saves the objects in Rdata format to the relevant directory.
    #' It handles versioning of models so all trained models are saved.
    
    folder_path <- dkuManagedFolderPathWithPartitioning(folder_name, partition_dimension_name)
    
    # create standard directory structure
    version_path <- file.path(folder_path, "versions", version_name)
    dir.create(version_path, recursive = TRUE)
    
    save(ts, file = file.path(version_path , "ts.RData"))
    save(df, file = file.path(version_path , "df.RData"))
    save(model_parameter_list, file = file.path(version_path , "params.RData"))
    save(model_list, file = file.path(version_path , "models.RData"))
}
