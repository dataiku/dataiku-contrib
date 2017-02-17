# Instagram plugin

This plugin offers a set of functions to pull data from the Instagram API. 

## Installation

This plugin can be installed directly from the DSS Plugin store. 

### Manual installation

N/A

## Usage

The plugin will let you pull data from the Instagram public API for 2 types of entities:  
* Accounts: given an Instagram account ID, the plugin will use the "/users" endpoint to collect metadata such as name, followers...
* Recent media: given an Instagram account ID, the plugin will use the "/users/.../media/recent" endpoint to collect metadata and statistics on the latest posts for the given users

Both methods assume that a valid access token is available and passed to the plugin, hence the access token must be created prior to using the plugin. Visit the Instagram documentation
for more information. 

The plugin adds the ability to pull data using:
* a DSS Dataset: useful when you need to manually get data for one or a few accounts 
* a DSS Recipe: useful when you need to get data for several accounts, stored in an existing DSS Dataset. 

## Changelog

* 0.9 - December 15th, 2016

	- The plugin is completely rewrote
	- Assumes access tokens are available
	- Adds Instagram datasets
	- Standardization of the code

* 0.0.1 - April 2016

	Initial release

## Known issues

N/A