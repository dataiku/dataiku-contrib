PLUGIN_VERSION=0.0.1
PLUGIN_ID=hdi-clusters

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-lib code-env web-app-templates python-exporters python-runnables

#include ../Makefile.inc
