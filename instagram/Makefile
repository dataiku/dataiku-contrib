PLUGIN_VERSION=0.9.2
PLUGIN_ID=instagram

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes js python-connectors resource

include ../Makefile.inc