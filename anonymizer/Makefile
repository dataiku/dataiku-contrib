PLUGIN_VERSION=0.0.2
PLUGIN_ID=anonymizer

clean:
	rm -rf dist build

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	ant clean jar
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json lib js

include ../Makefile.local.inc
