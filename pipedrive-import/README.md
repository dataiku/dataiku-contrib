# Pipedrive-import connector

This plugin provides a read connector to interact with [Pipedrive CRM](https://www.pipedrive.com) in [Dataiku Data Science Studio](http://www.dataiku.com/dss/). You can import deals, contacts and organizations.

## How to set-up

* Get a Pipedrive API key.
* Install python dependencies with the [pip of the DSS virtualenv](http://learn.dataiku.com/howto/code/python/install-python-packages.html): `data_dir/bin/pip install --upgrade requests awesome-slugify`
* Install the plugin in DSS.
* Create a new dataset with this connector. 

## Changelog

**Version 0.9.0 "beta" (2015-11-06)**

* New datasets: people and organizations
* Deals datasets renamed -> compatibility broken with 0.0.1
* Columns names are now suglified

**Version 0.0.1 "alpha" (2015-10-02)**

* Initial release: deals dataset

## Need help?

Ask your question on [answers.dataiku.com](https://answers.dataiku.com).
