# WORK IN PROGRESS

library(dataiku)
source(file.path(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastUtils.R"))
source(file.path(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastWrappers.R"))

input_folder_name = dkuCustomRecipeInputNamesForRole('input_folder')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()

load_from_managed_folder(input_folder_name)

config = dkuCustomRecipeConfig()

arima_forecast <- function(x, h){forecast(x, h = h, model = ARIMA_MODEL)}
e <- tsCV(TS_OUTPUT, arima_forecast, h=1)

print(e)