PLUGIN_VERSION=0.0.5
PLUGIN_ID=h2o

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-lib custom-recipes requirements.json

include ../Makefile.inc
