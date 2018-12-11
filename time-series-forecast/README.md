# Why this plugin?

**TL;DR:  to empower all Dataiku users to build a forecasting pipeline without code**

### 1. The use of forecasting

>Forecasting is required in many situations: deciding whether to build another power generation plant in the next five years requires forecasts of future demand; scheduling staff in a call centre next week requires forecasts of call volumes; stocking an inventory requires forecasts of stock requirements. Forecasts can be required several years in advance (for the case of capital investments), or only a few minutes beforehand (for telecommunication routing). Whatever the circumstances or time horizons involved, forecasting is an important aid to effective and efficient planning.
<p style="text-align: right"> - Hyndman, Rob J. and George Athanasopoulos</p>


Forecasting is a branch of Machine Learning where:
- The training data consists of one or multiple time series.
- The object to predict is the future of one of these time series.


A time series is simply a variable that changes and is measured across time.

### 2. The problem we are trying to solve

Forecasting is slightly different from "classic" Machine Learning (ML)  as currently available visually in Dataiku. It is mainly different because:
- Forecast models output a series of values whereas Visual ML outputs a single value.
- Forecasting model open source implementations are different from the Python and Scala ones available in the Visual ML, and cannot be integrated as a custom model in it.
- Evaluation of forecast accuracy uses specific methods (errors across a forecast horizon, cross-validation) which are not available in the Visual ML.

It is currently feasible to do time series forecasting in Dataiku using Visual ML, but it involves a lot of custom work: time series feature engineering to get lagged features for each time series, training one model for each forecast horizon, custom code to evaluate the models accuracy. Another way would be for a data scientist to code her own forecasting pipeline, most likely using open source R libraries. 

These two ways of building a forecasting pipeline require a lot of code. They are not accessible to an average user, be it an analyst or a junior data scientist.

With this plugin, we want to offer a simple and visual way to build a forecasting pipeline.

# How to use it?

This plugin offers a set of 3 visual recipes to forecast yearly to hourly time series.  It covers the full cycle of data cleaning, model training, evaluation and prediction. 

It follows classic forecasting methods described in Hyndman, Rob J., and George Athanasopoulos. *[Forecasting: principles and practice](https://otexts.org/fpp2)*. OTexts, 2018. and in Taylor, Sean J., and Benjamin Letham. *[Forecasting at Scale](https://doi.org/10.1080/00031305.2017.1380080)*. The American Statistician, 2018.


### 0. Install
The plugin can be installed from the public [dataiku-contrib](https://github.com/dataiku/dataiku-contrib/tree/time-series-forecast/time-series-forecast) git repo, by zipping the time-series-forecast folder and uploading it to the "Administration > Plugins > Advanced" section. Assuming the plugin passes user testing and code review, it will be possible to install the plugin visually from the "Administration > Plugins > Store" section. 

Note that the plugin uses an R code environment so R must be installed and integrated with Dataiku on your machine. 

You may encounter issues with the installation of the RStan package in the code environment on some operating systems . RStan has some system-level dependencies (C++) that may require additional setup. In this case, please see the [RStan Getting Started](https://github.com/stan-dev/rstan/wiki/RStan-Getting-Started) wiki.

### 1. Clean time series
*Resample, aggregate and clean the time series from missing values and outliers*

#### Input/output
Takes as input one dataset with time series data and outputs one dataset with the cleaned time series.

#### Settings
![Clean recipe screenshot](https://res.cloudinary.com/alexcbs/image/upload/v1544558081/clean_olpo1a.png)

1. Choose your input columns:
    - the column with time information in Dataiku date format (may need parsing beforehand in a Prepare recipe),
    - the columns with time series numeric values.
2. Choose how to aggregate and resample each time series: 
     - by which time granularity,
     - using which aggregation method (sum or average).
3. Choose how to handle missing values: 
     - interpolate (default), 
     - replace with average/median/fixed value, 
     - do nothing.
4. Choose how to handle outliers: 
     - interpolate, 
     - replace with average/median/fixed value,
     - do nothing (default). 

Outliers are detected by fitting a simple seasonal trend decomposition model using the [tsclean method](https://www.rdocumentation.org/packages/forecast/versions/8.4/topics/tsclean) from the forecast package.


### 2. Train models and evaluate errors on historical data
*Train forecasting models and evaluate them on historical data using temporal split or cross-validation*

#### Input/output
Takes as input one dataset with time series data (preferably the output of the previous Clean recipe) and outputs one folder to store forecasting R objects and one dataset with the evaluation results. 

#### Settings
![Train and evaluate recipe screenshot](https://res.cloudinary.com/alexcbs/image/upload/v1544558081/trainevaluate_qkaboo.png)

1. Choose your input columns:
    - the column with time information in Dataiku date format,
    - the target column with time series numeric values to forecast.
2. Choose how to build your forecasting models
    - *Automated mode (default):* Select which models to train. By default we only try two model types: Baseline and Prophet, as they converged for all the datasets used in our benchmarks. You may select more models, but be aware that some model types take more time to compute, or may fail to converge on datasets. In the latter case, you will get an error when running the recipe, telling you which model type to deactivate.
    - *Expert mode:* to gain access to advanced parameters that are custom to each model type.
3. Choose how to evaluate all models
    - *Split (default):* Train/test split where the test set consists of the last H values of the time series. You can change H with the horizon parameter. The models will be retrained on the train split and evaluated on their errors on the test split, for the entire forecast horizon.
    - *Cross-validation:* Time series method to split your dataset into multiple rolling train/test splits. The models will be retrained and evaluated on their errors for each split. Error metrics are then averaged across all splits. Each split is defined by a cutoff date: the train split is all data before or at the cutoff date, and the test split is the H values after cutoff. H is the horizon parameter, same as for the Split strategy. Cutoffs are made at regular intervals according to the "Cutoff period" parameter, but cannot be before the "Initial training" parameter. Having a large enough initial training set guarantees that the models trained on the first splits have enough data to converge. You may want to increase that parameter if you encounter model convergence errors.
    
The exact method used for cross-validation  is described in the [Prophet documentation](https://facebook.github.io/prophet/docs/diagnostics.html) and explained in a slightly longer version [here](https://robjhyndman.com/hyndsight/tscv). 

Note that cross-validation takes more time to compute since it involves as multiple retraining and evaluation of models. In contrast, the Split strategy only requires one retraining and evaluation. In order to alleviate that problem, we implemented retraining so that models are refit to each training split but hyperparameters are not re-estimated. This is done on purpose to accelerate the computation. 


### 3. Forecast future values and get historical residuals
*Use previously trained models to predict future values and/or get historical residuals*

#### Input/output
Takes as input the model folder and the evaluation dataset from the previous Train and Evaluate recipe, and outputs one dataset with forecasts.

#### Settings
![Forecast recipe screenshot](https://res.cloudinary.com/alexcbs/image/upload/v1544558081/forecast_wlk93n.png)

1. Choose how to select the model used for prediction:
    - Automatic if you want to select the best model according to an error metric computed in the evaluation dataset input,
    - Manual to select a model yourself
2. Choose whether you want to include the history, the forecast, or both.
3. If you are including the forecast, specify the horizon and the probability percentage for the confidence interval.

#### Visualization

The output dataset of this recipe is a good candidate for the user to build charts to visually inspect the forecast results. Please see an example of such chart below:
![History and forecast screenshot](https://res.cloudinary.com/alexcbs/image/upload/v1543354646/Screenshot_2018-11-27_at_21.36.55_ay8ccd.png)


### Note on partitioning (advanced usage)

If you want run the recipes to get multiple forecasting models per category (e.g. per product or store), you will need partitioning. That requires to have all datasets partitioned by 1 dimension for the category, using the [discrete dimension](https://doc.dataiku.com/dss/latest/partitions/identifiers.html#discrete-dimension-identifiers) feature in Dataiku. If the input data is not partitioned, you can use a Sync recipe to repartition it, as explained in [this article](https://www.dataiku.com/learn/guide/other/partitioning/partitioning-redispatch.html).


### To infinity and beyond

As an option to go further, a full pipeline could add Visual ML after the forecasting pipeline based on the plugin. The idea would be to predict the time series residuals (actual value - forecast) using ML models. ML is indeed most effective once the trend and seasonality have been removed. It would allow to integrate additional numeric or categorical predictors in the pipeline. 

Anomaly detection can also be performed using clustering in the Visual ML. The idea would be to detect spikes in the residuals values and/or other numeric or categorical variables.
