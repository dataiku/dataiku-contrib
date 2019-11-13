PLUGIN_VERSION=1.0.0
PLUGIN_ID=dataiku-project-bundle-migration

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-runnables

include ../Makefile.inc
