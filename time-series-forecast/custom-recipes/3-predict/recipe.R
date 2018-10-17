# WORK IN PROGRESS NOT READY!

library(dataiku)
source(file.path(dataiku:::dkuCustomRecipeResource(), "dkuTimeSeriesForecast.R"))

eval_dataset_name = dkuCustomRecipeInputNamesForRole('eval_dataset')[1]
model_folder_name = dkuCustomRecipeInputNamesForRole('model_folder')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()
print(config)
for(n in names(config)){
    assign(n, config[[n]])
}

.LEVEL <- c(CONFIDENCE_INTERVAL_LEVEL_1, CONFIDENCE_INTERVAL_LEVEL_2)

load_from_managed_folder(model_folder_name)