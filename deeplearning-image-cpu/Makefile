PLUGIN_VERSION=0.1.0
PLUGIN_ID=deeplearning-image-cpu

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip code-env custom-recipes js plugin.json python-lib python-runnables resource web-app-templates

include ../Makefile.inc