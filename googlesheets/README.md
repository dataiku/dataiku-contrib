# Google Sheets connector

This plugin provides a read and write connector to interact with [Google Sheets](https://www.google.com/intl/en_us/sheets/about/) documents, that are commonly named spreadsheets.

## How to set-up

* Install the plugin in Dataiku DSS. The plugin requires the installation of a code env.
* Get a `Service account` from the Google API Console with the `Sheets API` enabled. Export the credentials as a `JSON` file.
* Create a new dataset with this connector. Fill the 4 parameters and click on the `Test & Get schema` button. Then, `save` and `explore`.
* Share the spreadsheet with the email of the service account (likely in a similar form of `...@developer.gserviceaccount.com`).

## How to get the JSON Service Account

The plugin uses the protocol [OAuth 2.0 for Server to Server Applications](https://developers.google.com/identity/protocols/OAuth2ServiceAccount) to connect to the Google Sheets API.

In order to use the plugin, you will need service account's credentials exported in a JSON file with the Sheets API enabled. To generate this JSON, you can follow [the instructions on the plugin page](https://www.dataiku.com/community/plugins/info/googlesheets.html).

## Changelog

* 1.0.0 - December 18th, 2018

    - [New] The plugin now uses a [Code env](https://doc.dataiku.com/dss/latest/code-envs/index.html) so that required libraries are isolated.
    - [Fix] The order of the columns is now preserved
    - [Enhancement] The plugin relies on Google Sheets API v4 and does not have hard-coded limits on volume any more (ie. it handles what the API can handle)
    - [Enhancement] More understandable errors messages (for example, the plugin will let the user know if the spreadsheet has not been shared with the service account, or if the sheet name is invalid)
    - [Enhancement] Instructions on the plugin page to get the service account's credentials

* 0.1.0 - February 20th, 2017

	- Add write support
	- Add support for oauth2client >= 2.0.0

* 0.0.1 - November 5th, 2015

	Initial release

## Improvement ideas

* Add ability to import all sheets from a unique spreadsheet
* Add a new connector to list all spreadsheets available from an account

## Need help?

Find out more on the [plugin page](https://www.dataiku.com/community/plugins/info/googlesheets.html) on Dataiku's website.

Ask your question on [answers.dataiku.com](https://answers.dataiku.com). Or, [open an issue](https://github.com/dataiku/dataiku-contrib/issues).
