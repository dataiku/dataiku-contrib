# Forecast.io plugin

This plugin allows you to get weather data in Dataiku DSS provided by [Forecast.io API](https://developer.forecast.io).

You will need a Forecast.io API key to use this plugin. A free plan allow you to make 1,000 calls per day. After that, each API call costs $0.0001.

Units (for example Fahrenheit or Celsius Degrees) are automatically chosen, based on geographic location.

Times are UNIX timestamps (signed integers), local time is assumed.

## Changelog

**Version 0.1.0 (2016-06-17)**

* Initial release: custom dataset to get historical weather data for a specific location.

## Improvement ideas

* Create a custom recipe to get forecasts, given latitude/longitude data.
* Improve the localization (timezones, units, etc.)

## Need help?

Read [Forecast.io's documentation](https://developer.forecast.io/docs/v2).

Ask your question on [answers.dataiku.com](https://answers.dataiku.com). Or, [open an issue](https://github.com/dataiku/dataiku-contrib/issues).


