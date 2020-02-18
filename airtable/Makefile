PLUGIN_VERSION=0.1.4
PLUGIN_ID=airtable

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-connectors python-lib code-env

include ../Makefile.inc