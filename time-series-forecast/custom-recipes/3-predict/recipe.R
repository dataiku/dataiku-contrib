# WORK IN PROGRESS
library(dataiku)
R_lib_path <- paste(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastUtils.R", sep="/")
source(R_lib_path)

input_dataset_name = dkuCustomRecipeInputNamesForRole('input_dataset')[1]
input_folder_name = dkuCustomRecipeOutputNamesForRole('input_folder')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
CONFIDENCE_INTERVAL_LEVEL_1 <- config[["CONFIDENCE_INTERVAL_LEVEL_1"]]
CONFIDENCE_INTERVAL_LEVEL_2 <- config[["CONFIDENCE_INTERVAL_LEVEL_2"]]
CONFIDENCE_LEVEL_BOOTSTRAP_ACTIVATED <- config[["CONFIDENCE_LEVEL_BOOTSTRAP_ACTIVATED"]]

.LEVEL <- c(CONFIDENCE_INTERVAL_LEVEL_1, CONFIDENCE_INTERVAL_LEVEL_2)

load_from_managed_folder(input_folder_name)