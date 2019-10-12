PLUGIN_VERSION=0.0.3
PLUGIN_ID=wms-maps-backgrounds

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json js parameter-sets

include ../Makefile.inc
