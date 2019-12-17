PLUGIN_VERSION=0.1.0
PLUGIN_ID=movies-apis

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes code-env

include ../Makefile.inc
