# Dark Sky (previously Forecast.io) plugin

This plugin allows you to get weather data in Dataiku DSS provided by [Dark Sky API](https://darksky.net/dev) (previously Forecast.io).

You will need a Dark Sky API key to use this plugin. A free plan allow you to make 1,000 calls per day. After that, each API call costs $0.0001.

The International System of Units (abbreviated as SI) is used.

Times are UNIX timestamps (signed integers), local time is assumed.

## Changelog

**Version 0.2.0 (2018-09-07)**

* Plugin renamed because `Forecast.io` became `Dark Sky`
* Retrieves forecasts in addition to historical data
* Caching system: now optional + more efficient
* Default to SI units

**Version 0.1.0 (2016-06-17)**

* Initial release: custom dataset to get historical weather data for a specific location.

## Improvement ideas

* Create a recipe to get weather conditions given a latitude/longitude column.
* Improve the localization (timezones, units, etc.)

## Need help?

Read [Dark Sky's documentation](https://darksky.net/dev/docs).

Ask your question on [answers.dataiku.com](https://answers.dataiku.com). Or, [open an issue](https://github.com/dataiku/dataiku-contrib/issues).


