PLUGIN_VERSION=0.3.0
PLUGIN_ID=forecastio

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-connectors code-env

include ../Makefile.inc
