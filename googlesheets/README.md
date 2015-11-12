# Google Sheets connector

This plugin provides a read (and soon write) connector to interact with [Google Sheets](https://www.google.com/intl/en_us/sheets/about/) documents, that are commonly named spreadsheets.

## How to set-up

* Install python dependencies with the [pip of the DSS virtualenv](http://learn.dataiku.com/howto/code/python/install-python-packages.html): `data_dir/bin/pip install --upgrade gspread oauth2client PyOpenSSL awesome-slugify`
* Get a JSON file containing a “Service account” from the [Google Developers Console](https://console.developers.google.com/project) with the “Drive API“ enabled.
* Install the plugin in DSS.
* Create a new dataset with this connector. Fill the 4 parameters and click on the “Test & Get schema“ button. Then, “save“ and “explore“.
* Remember to share the spreadsheet with the email of the service account (`536772...-fezerf...@developer.gserviceaccount.com`).

## Improvement ideas

* Add ability to write to a sheet
* Add ability to import all sheets from a unique spreadsheet
* Add a new connector to list all worksheets available from an account

## Know issues

**Even with dependencies installed, there are missing modules.**

Try to upgrade your pip: `data_dir/bin/pip install --upgrade pip`

**I get some errors when installing Python dependencies.**

The resolution depends on your OS. Google it or try [this thread on Stackoverflow](http://stackoverflow.com/questions/22073516/failed-to-install-python-cryptography-package-with-pip-and-setup-py).

**I get an error `WorksheetNotFound`.**

Check the document id, the sheet name (probably different from the worksheet name) and that you shared the document with the service account email.

**I get an error `com.google.gson.stream.MalformedJsonException: Unterminated object at line 1 column ...`.**

It is a bug in DSS 2.1.x. Upgrade to DSS 2.2.0 or later.

**Some old and empty columns remain in my dataset.**

Try to delete all the columns in `Settings>Schema` then click on `Test & Get schema` again in `Settings>Connector` to regenerate a new schema.

## Need help?

Find out more on the [plugin page](https://www.dataiku.com/community/plugins/info/googlesheets.html) on Dataiku's website.

Ask your question on [answers.dataiku.com](https://answers.dataiku.com). Or, [open an issue](https://github.com/dataiku/dataiku-contrib/issues).
