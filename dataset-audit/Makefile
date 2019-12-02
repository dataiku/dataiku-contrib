PLUGIN_VERSION=0.0.4
PLUGIN_ID=dataset-audit

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes code-env

include ../Makefile.inc
