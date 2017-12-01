PLUGIN_VERSION=0.0.1
PLUGIN_ID=compare-datasets

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes

include ../Makefile.inc

