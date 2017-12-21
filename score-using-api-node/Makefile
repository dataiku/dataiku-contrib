PLUGIN_VERSION=1.0.0
PLUGIN_ID=score-using-api-node

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes

include ../Makefile.inc

