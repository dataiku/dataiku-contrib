PLUGIN_VERSION=0.2.5
PLUGIN_ID=census-us

plugin:
	cat plugin.json|json_pp > /dev/null
	cat custom-recipes/*/recipe.json|json_pp > /dev/null
	for f in python-connectors/*; do echo $$f; cat $$f/connector.json|json_pp > /dev/null;done
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes python-connectors python-lib requirements.json
    
include ../Makefile.inc
