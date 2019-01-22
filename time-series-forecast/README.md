# Why this plugin?

The Forecast plugin provides visual recipes in Dataiku DSS to work on time series data to solve forecasting problems.

>Forecasting is required in many situations: deciding whether to build another power generation plant in the next five years requires forecasts of future demand; scheduling staff in a call centre next week requires forecasts of call volumes; stocking an inventory requires forecasts of stock requirements. Forecasts can be required several years in advance (for the case of capital investments), or only a few minutes beforehand (for telecommunication routing). Whatever the circumstances or time horizons involved, forecasting is an important aid to effective and efficient planning.
<p style="text-align: right"> - Hyndman, Rob J. and George Athanasopoulos</p>

This plugin offers a simple and visual way to build a forecasting pipeline, in 3 different steps:
- Cleaning, aggregation, and resampling of time series data, i.e. data of one or several values measured over time
- Training of forecasting models of time series data, and evaluation of these models
- Predicting future values based on trained models

The following models are available in the recipe:
- Prophet
- Neural Network
- Seasonal Trend
- Exponential Smoothing
- ARIMA

This plugin does NOT work on narrow temporal dimensions (data must be at least at the hourly level), does not provide signal processing techniques (Fourier Transform…).

This plugin works well when:
- The training data consists of one or multiple time series at the hour, day, week, month or year level and fits in server’s RAM.
- The object to predict is the future of one of these time series.

Forecasting is slightly different from "classic" Machine Learning (ML)  as currently available visually in Dataiku. It is mainly different because:
- Forecast models output a series of values whereas Visual ML outputs a single value.
- Forecasting model open source implementations are different from the Python and Scala ones available in the Visual ML, and cannot be integrated as a custom model in it.
- Evaluation of forecast accuracy uses specific methods (errors across a forecast horizon, cross-validation) which are not available in the Visual ML.

Forecasting is a branch of Machine Learning where:
- The training data consists of one or multiple time series.
- The object to predict is the future of one of these time series.

# How to use it?

This plugin offers a set of 3 visual recipes to forecast yearly to hourly time series.  It covers the full cycle of data cleaning, model training, evaluation and prediction. 

It follows classic forecasting methods described in Hyndman, Rob J., and George Athanasopoulos. *[Forecasting: principles and practice](https://otexts.org/fpp2)*. OTexts, 2018. and in Taylor, Sean J., and Benjamin Letham. *[Forecasting at Scale](https://doi.org/10.1080/00031305.2017.1380080)*. The American Statistician, 2018.

Please read [the plugin page](https://www.dataiku.com/plugins/) on Dataiku's website for more instructions on the usage of the plugin.

You can also ask your questions on our Q&A, [answers.dataiku.com](https://answers.dataiku.com), or open an [Github issue](https://github.com/dataiku/dataiku-contrib/issues).

# License

The Forecast plugin is:

   Copyright (c) 2018 Dataiku SAS
   Licensed under the [MIT License](LICENSE.md).
