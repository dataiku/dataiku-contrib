# Plugin Information

[import.io](https://www.import.io/) lets users automatically turn Web pages into data, thanks to its powerful and very easy to use scraping and parsing technology.

This plugin offers advanced connectivity to import.io scrappers. By using the import.io plugin, you can easily retrieve data hidden in web pages, or enrich existing datasets with external web data.

The plugin import.io plugin can:

- Retrieve data from a single import.io API using the **dataset**
- Bulk-enrich a dataset containing URLs, repeateadly getting data from an import.io extractor on each URL, using the **recipe**

The import.io plugin offers connectivity thanks to 3 different components:

#### Dataset for single API

The **Import.io dataset** is the simplest integration. It calls the import.io once and populates a dataset with the results.

Use this to fetch structured data from a single page.

Start by defining your extractor in import.io, then create the dataset and paste the import.io API URL into the dataset configuration.

#### Recipes for bulk enrich

The enrichment recipes are used to enrich a dataset: for each row of the input dataset, this recipe reads the URL in a given column, calls import.io’s API with it, and writes the results to the output dataset. This way of repeatedly calling the API to retrieve data is sometimes called “Bulk extract” or “Chain API” on import.io website.

Start by defining your extractor on one example page in import.io, then create the recipe.

A great way to use this is together with the **editable datasets** in Dataiku.

The “Connector” recipe is also used for bulk enrich. To get new data in Import.io, one has the choice between “Magic”, “Extractor”, “Crawler” or the more advanced “Connector”. This recipe allows to request an API created with the last one.

# Changelog

## Version 1.0.0 (2016/06/28)

* Add support for new "extraction.import.io" API