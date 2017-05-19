This Dataiku plugin enriches  data via the ESRI(r) Acrgis.com(r) API. This plugin covers the following ESRI(r)'s API features:

 * World Geocoder
 * Enrich from XY coordinates
 * Enrich from Layer (like postcode). Corresponding to "Named statistical areas" into the ESRI(r) documentation.

Two utilities are also provided:

 *  "Show Enrichment API coverage" dataset: shows the countries and data collections provided by this plugin. This may help the user to check if a country is supported and check the main datacollections
 * "Get catalog content" recipe: this recipe is needed to find a specific layer or data collection.


# Note about "Get catalog content" recipe

In this recipe you can either:

	* Use a "Show enrichment API coverage" dataset as input
	* Put a country selection (in isocode2 format) as a Python list

The output of this recipe is required for the enrichment tasks since the ESRI API needs to have specific layer and datacollection identifiers

# How to get a login / password ?

* go to arcgis.com and open an ArcGis online Account.
* In the ArcGis online settings > Security. Make sure that the parameter "... Only authorize acces via https:// ..." is checked on.
* Put your login / password into the DSS ESRI-data-enrichment plugin when this is required.

# About the layer enrichment

Batch size :

	* The request sent to the service cannot be greater than 10mb.
	* Currently, a maximum of 100 features can be enriched with the service per request.
    
# IMPORTANT NOTICE:
The data collections have been partially changed [official doc not updated: https://developers.arcgis.com/rest/geoenrichment/api-reference/data-collections.htm ] and the API doesn't provide a correspondance between the last datacollections name and the new ones, 
In the current version, the plugin can't get the last name of the Esri's datacollections since the API doesn't provide this information directly  
some countries may not be supported, check the log dataset.
We'll find to get the last datacollection name in a next release.