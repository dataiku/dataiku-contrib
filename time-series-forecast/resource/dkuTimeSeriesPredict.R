# Functions used for the Forecast recipe

library(forecast)
library(R.utils)
library(dataiku)
source(file.path(dkuCustomRecipeResource(), "dkuPluginUtils.R"))

load_from_managed_folder <- function(folder_id){
    input_folder_path <- dkuManagedFolderPath(folder_id)
    input_folder_type <- tolower(dkuManagedFolderInfo(folder_id)[["info"]][["type"]])
    if(input_folder_type!="filesystem") {
         stop("Input folder must be on the Server Filesystem. \
         Please use the \"filesystem_folders\" connection.")
    }
    last_version_timestamp <- max(list.files(file.path(input_folder_path, "versions")))
    last_version_date <- as.POSIXct(as.numeric(last_version_timestamp)/1000, origin="1970-01-01") %>%
        (function(x) strftime(x , dku_date_format, "UTC"))
    assign("LAST_VERSION_DATE", last_version_date, envir = .GlobalEnv)
    version_path <- file.path(input_folder_path, "versions", last_version_timestamp)
    models_path <- file.path(version_path, "models")
    
    rdata_path_list <- list.files(
        path = version_path,
        pattern = "*.RData",
        full.names = TRUE,
        recursive = TRUE
    )
    for(rdata_path in rdata_path_list){
        load(rdata_path, envir = .GlobalEnv)
    }
    plugin_print("Models, time series and parameters loaded from folder")
}
