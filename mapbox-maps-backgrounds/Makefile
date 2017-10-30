PLUGIN_VERSION=0.0.1
PLUGIN_ID=mapbox-maps-backgrounds

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json js

include ../Makefile.inc