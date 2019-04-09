PLUGIN_VERSION=1.1.1
PLUGIN_ID=microsoft-power-bi

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-lib code-env web-app-templates python-exporters python-runnables

include ../Makefile.inc