PLUGIN_VERSION=0.9.5
PLUGIN_ID=spss-format

plugin:
	ant jar
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json java-lib java-formats --exclude '*.java' '*.sav'

include ../Makefile.inc
