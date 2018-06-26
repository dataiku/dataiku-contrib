# WORK IN PROGRESS

library(dataiku)
source(file.path(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastUtils.R"))
source(file.path(dataiku:::dkuCustomRecipeResource(), "dkuTSforecastWrappers.R"))

input_folder_name = dkuCustomRecipeInputNamesForRole('input_folder')[1]
output_dataset_name = dkuCustomRecipeOutputNamesForRole('output_dataset')[1]

config = dkuCustomRecipeConfig()

load_from_managed_folder(input_folder_name)

config = dkuCustomRecipeConfig()

TEST_SIZE <- 10

train_set <- head(TS_OUTPUT, length(TS_OUTPUT) - TEST_SIZE)
test_set <- tail(TS_OUTPUT, TEST_SIZE)

naive_forecast <- naive_forecast_function(train_set, h=TEST_SIZE)
print(accuracy(naive_forecast, test_set))
e <- tsCV(TS_OUTPUT, naive_forecast_function, h=2)
print(head(e,20))
 
seasonal_trend_forecast <- seasonal_trend_forecast_function(train_set, h=TEST_SIZE)
print(accuracy(seasonal_trend_forecast, test_set))
e <- tsCV(TS_OUTPUT, seasonal_trend_forecast_function, h=2)
print(head(e,20))

neuralnetwork_forecast <- neuralnetwork_forecast_function(train_set, h=TEST_SIZE)
print(accuracy(neuralnetwork_forecast, test_set))
e <- tsCV(TS_OUTPUT, neuralnetwork_forecast_function, h=2)
print(head(e,20))

arima_forecast <- arima_forecast_function(train_set, h=TEST_SIZE)
print(accuracy(arima_forecast, test_set))
e <- tsCV(TS_OUTPUT, arima_forecast_function, h=2)
print(head(e,20))

statespace_forecast <- statespace_forecast_function(train_set, h=TEST_SIZE)
print(accuracy(statespace_forecast, test_set))
e <- tsCV(TS_OUTPUT, statespace_forecast_function, h=2)
print(head(e,20))