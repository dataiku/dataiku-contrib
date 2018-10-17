# Various utility functions used across the plugin for non-specific time series stuff

# This is the character date format used by Dataiku DSS for dates as per the ISO8601 standard
dku_date_format = "%Y-%m-%dT%T.000Z"

# This fixed mapping is used to convert time series from R data.frame to forecast::msts format
# This is needed as the forecast::msts format requires seasonality arguments
map_granularity_seasonality <- list(
    year = c(1),  # no seasonality
    quarter = c(4), # seasonality within year
    month = c(12), # seasonality within year
    week = c(365.25/7), # seasonality within year
    day = c(365.25, 7), # seasonality within year, week
    hour = c(8766, 168, 24) # seasonality within year, week, day
)


plugin_print <- function(message){
    print(paste("[PLUGIN_LOG]", message))
}


clean_list_mixed_type <- function(named_list){
    #' Infers types for a named list
    #'
    #' @description This returns a list with inferred types. 
    #' First it checks for numeric type, then boolean, and finally defaults to character
    
    cleaned_list <- list()
    for(name in names(named_list)){
        if(!is.na(as.numeric(named_list[[name]]))){
            cleaned_list[[name]] <- as.numeric(named_list[[name]])
        }
        else{
            if(!is.na(as.logical(named_list[[name]]))){
                cleaned_list[[name]] <- as.logical(named_list[[name]])
            }
            else{
                cleaned_list[[name]] <- as.character(named_list[[name]])
            }
            
        }
    }
    return(cleaned_list)
}


#clean_kwargs_from_param <- function(kwargs_param) {
#    clean_kwargs <- clean_list_mixed_type(as.list(kwargs_param)) # handles map parameter raw format
#    if(length(clean_kwargs) != 0){
#        plugin_print("Additional parameters are below")
#        print(clean_kwargs)
#    }
#    return(clean_kwargs)
#}