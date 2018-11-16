PLUGIN_VERSION=1.0.0
PLUGIN_ID=sentence-embedding

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json code-env custom-recipes python-lib python-runnables

include ../Makefile.inc