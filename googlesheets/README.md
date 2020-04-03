# Google Sheets plugin

This plugin provides read and write interactions with [Google Sheets](https://www.google.com/intl/en_us/sheets/about/) documents, that are commonly named spreadsheets.

It contains:

* a dataset component:
    - to read a sheet as a dataset in Dataiku DSS
    - to write a dataset into a sheet
* a recipe component: to append rows (from a dataset) to a sheet

## How to set-up

* Install the plugin in Dataiku DSS. The plugin requires the installation of a code env.
* Create a `Service Account` in the Google API Console with the `Sheets API` enabled. Export the credentials as a `JSON` token file.
* Create a new dataset or new recipe with this plugin.

## Some tips

* When using the dataset component, it is important to click on the button `Test & Get schema` to set-up the schema.
* A dataset created with the plugin can be placed as the output of a recipe. In this case, the plugin will write data to the spreadsheet when the recipe is run ("write mode").
* The plugin exposes two modes used by Google Sheets to interpret values format: [RAW and USER_ENTERED](https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption)

## How to get the Service Account token

The plugin uses the protocol [OAuth 2.0 for Server to Server Applications](https://developers.google.com/identity/protocols/OAuth2ServiceAccount) to connect to the Google Sheets API.

In order to use the plugin, you will need Service Account's credentials exported in a JSON token file with the Sheets API enabled. To generate this JSON, you can follow [the instructions on the plugin page](https://www.dataiku.com/product/plugins/googlesheets/).

Don't forget share the spreadsheet with the email of the service account (likely in a similar form of `...@developer.gserviceaccount.com`).

## Need help?

Find out more on the [plugin page](https://www.dataiku.com/product/plugins/googlesheets/) on Dataiku's website.

Ask your question on [community.dataiku.com](https://community.dataiku.com). Or, [open an issue](https://github.com/dataiku/dataiku-contrib/issues).

## Changelog

* 1.1.0 - In progress
    
    - [New] A recipe is now available to append rows to a sheet (it does not modify the preexisting values)
    - [New] When writing data to a spreadsheet, two modes to interpret values format: RAW and USER_ENTERED
    - [New] A sheet can be read in JSON format (schema-less)
    - [Enhancement] Python 3 compatible
    - [Enhancement] Dependencies update: gspread upgrade, use of python-slugify instead of awesome-slugify

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
