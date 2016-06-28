This plugin offers connectivity to <a href="https://import.io/">import.io</a> thanks to:

* the **dataset** “Import.io dataset” (it calls import.io once and populates a dataset with the results),
* the **recipe** “Extractor / Magic”. This recipe enriches a dataset: for each row of the input dataset, this recipe reads the URL in a given column, calls import.io's API with it, and writes the results to the output dataset. This way of repeatedly calling the API to retrieve data is sometimes called  “Bulk extract” or “Chain API” on import.io website.
* the **recipe** “Connector”. Indeed, in Import.io, to get new data one has the choice between “Magic”, “Extractor”, “Crawler” or the more advanced “Connector”. This recipe allows to connect to the last one.

# Changelog

## Version 1.0.0 (2016/06/28)

* Add support for new "extraction.import.io" API