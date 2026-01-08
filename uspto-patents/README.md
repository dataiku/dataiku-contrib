# US Patents dataset

This plugin provides a mechanism to download the US patents datasets from Google, as described [here](https://cloud.google.com/blog/products/gcp/google-patents-public-datasets-connecting-public-paid-and-private-patent-data), as a XML file.

This connector retrieves that data

Since these XML files are not well-formed, this connector provides built-in cleansing and parsing. The resulting dataset contains one “patent” column (JSON) containing the patent metadata, abstract, description and claims

The connector enables a local folder cache to simplify your developments. You can choose to retrieve the whole patent database (beware it’s big : 40GB) or any year between 2005 and 2015

The user can choose a partitioning strategy before building the dataset.

You need to install the dependencies of the plugin. Go to the **Administration > Plugins** page to get the command-line to install dependencies
