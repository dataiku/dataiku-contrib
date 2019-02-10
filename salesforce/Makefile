PLUGIN_VERSION=1.1.1
PLUGIN_ID=salesforce

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-connectors python-lib custom-recipes

include ../Makefile.inc