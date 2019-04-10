PLUGIN_VERSION=0.2.8
PLUGIN_ID=census-us

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes python-connectors python-lib requirements.json
    
include ../Makefile.inc