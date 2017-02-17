# Algolia Search connector

This plugin provides a read/write connector to interact with [Algolia Search](https://www.algolia.com)

With this plugin, you can:

* Import a complete Algolia index as a [DSS](https://www.dataiku.com/dss) dataset
* Copy a DSS dataset to an Algolia index

You could thus use a DSS [Flow](http://doc.dataiku.com/dss/latest/flow/index.html) to prepare your 
Algolia search index.

# Changelog

## Version 0.0.4 (2016/06/28)

* Truncate to 8K chars instead of 5K
* Minor wording fixes

# Improvement ideas

* Add ability to put a query to restrict what is being fetched
* Add ability to use facets instead of simply documents
* Add another connector to fetch the Algolia query logs
* Add another connector to retrieve the 0-results queries from Algolia analytics API
