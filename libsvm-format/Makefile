PLUGIN_VERSION=1.0.0
PLUGIN_ID=libsvm-format

plugin:
	ant
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json java-lib java-formats --exclude '*.java' '*.svm'

include ../Makefile.inc
