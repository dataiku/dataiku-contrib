PLUGIN_VERSION=1.1.0
PLUGIN_ID=named-entity-recognition

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json code-env custom-recipes python-lib python-runnables web-app-templates

include ../Makefile.inc