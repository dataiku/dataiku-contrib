This custom recipe gets the US Census block group and the tract id corresponding to the Latitude Longitude provided. 
More info here: https://www.census.gov/geo/reference/

It uses the free US Census API (it's free, expect low performance)

Output:
If the Latitude / Longitude does not exist in the US Census API then no records are returned.

Note:
* In the previous version the API used was FCC.gov. This API has been decommissioned in June 2019, some fields like block_id are no more available.
* This plugin will be merged and no more supported in the coming weeks with the Dataiku DSS US Census Plugin: https://www.dataiku.com/dss/plugins/info/us-census.html

