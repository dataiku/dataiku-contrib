# Forecast Plugin

The Forecast plugin provides visual recipes in Dataiku DSS to work on time series data to solve forecasting problems.

>Forecasting is required in many situations: deciding whether to build another power generation plant in the next five years requires forecasts of future demand; scheduling staff in a call centre next week requires forecasts of call volumes; stocking an inventory requires forecasts of stock requirements. Forecasts can be required several years in advance (for the case of capital investments), or only a few minutes beforehand (for telecommunication routing). Whatever the circumstances or time horizons involved, forecasting is an important aid to effective and efficient planning.
<p style="text-align: right"> - Hyndman, Rob J. and George Athanasopoulos</p>


## Scope of the plugin

This plugin offers a set of 3 visual recipes to forecast yearly to hourly time series. It covers the full cycle of data cleaning, model training, evaluation and prediction.
- Cleaning, aggregation, and resampling of time series data
- Training of forecast models of time series data, and evaluation of these models
- Predicting future values and get historical residuals based on trained models

The following models are available in the recipe:
- Neural Network
- Seasonal Trend
- Exponential Smoothing
- ARIMA

This plugin does NOT work on narrow temporal dimensions (data must be at least at the hourly level) and does not provide signal processing techniques (Fourier Transform…).

This plugin works well when:
- The training data consists of one or multiple time series at the hour/day/week/month/quarter/year level and fits in the server’s RAM.
- The object to predict is the future of one of these time series.

## When to use this plugin

Forecasting is a branch of Machine Learning where:
- The training data consists of one or multiple time series.
- The object to predict is the future values of one of these time series.

A time series is simply a variable with several values measured over time.

Forecasting is slightly different from "classic" Machine Learning (ML) as available currently in the Visual ML interface of Dataiku, because:
- Forecast models output multiple values whereas one Visual ML analysis is designed to predict a single output.
- Open source implementations of forecast models are different from the Python/Scala ones available in Visual ML.
- Evaluation of forecast accuracy uses specific methods (errors across a forecast horizon, cross-validation) which are not currently available in Visual ML.

Having said that, it has always been possible to forecast time series in Dataiku using Visual ML with custom work:
- Feature engineering to get lagged features for each time series, for instance using the Window recipe.
- If the forecast is for more than one time step ahead: training one Visual ML model for each forecast horizon.
- Custom code to evaluate the models accuracy and forecast future values for multiple steps.

Another way would be for a data scientist to code her own forecasting pipeline using open source R or Python libraries.

These two ways of building a forecasting pipeline require good knowledge of machine learning, forecasting techniques and programming. They are not accessible to a Data Analyst user. With this plugin, we want to offer a simple way to build a forecasting pipeline without code.


## Installation and usage of the plugin

Please see [plugin page](https://www.dataiku.com/dss/plugins/info/forecast.html) on Dataiku's website.

Note that the plugin uses an R code environment so R must be installed and integrated with Dataiku on your machine (version 3.5.0 or above). Anaconda R is not supported.

## Changelog

**Version 0.3.0 "beta 3" (2019-05)**

* Remove dependency on rstan and prophet packages. 

**Version 0.2.0 "beta 2" (2019-03)**

* Multivariate Forecasting: Added support of external regressors for Neural Network, Prophet and ARIMA models (requires availability of future values of regressors when forecasting).

**Version 0.1.0 "beta 1" (2019-01)**

* Initial release
* First pipeline for univariate forecasting of hourly to yearly time series

## Roadmap

- Evaluation recipe:
     * For cross-validation strategy: error metrics at each step within the horizon
- Prediction recipe:
     * Add ability to get multiple model forecasts at the same time for ensembling
     * Fan plot of confidence intervals within the horizon

You can also check its development branch ["time-series-forecast"](https://github.com/dataiku/dataiku-contrib/tree/time-series-forecast/time-series-forecast) on the [dataiku-contrib](https://github.com/dataiku/dataiku-contrib) git repo.

You can also ask your questions on our Q&A, [answers.dataiku.com](https://answers.dataiku.com), or open an [Github issue](https://github.com/dataiku/dataiku-contrib/issues).


## Advanced Usages

### Forecasts by Entity

If you want run the recipes to get multiple forecast models per entity (e.g. per product or store), you will need partitioning. That requires to have all datasets partitioned by 1 dimension for the category, using the [discrete dimension](https://doc.dataiku.com/dss/latest/partitions/identifiers.html#discrete-dimension-identifiers) feature in Dataiku. If the input data is not partitioned, you can use a Sync recipe to repartition it, as explained in [this article](https://www.dataiku.com/learn/guide/other/partitioning/partitioning-redispatch.html).

### Combination of Forecast and Machine Learning

A full pipeline would combine ML with forecast models. First, you can predict the forecast residuals (actual value - forecast) using ML models. ML is indeed most effective once the trend and seasonality have been removed, so the time series is stationary. Second, you can perform anomaly detection using clustering in the Visual ML, by detecting spikes in the forecast residuals.

# License

The Forecast plugin is:

   Copyright (c) 2019 Dataiku SAS
   Licensed under the [MIT License](LICENSE.md).
