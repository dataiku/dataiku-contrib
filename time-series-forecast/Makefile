PLUGIN_VERSION=0.3.0
PLUGIN_ID=time-series-forecast

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json code-env custom-recipes resource

include ../Makefile.inc
