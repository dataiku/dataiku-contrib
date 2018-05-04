PLUGIN_VERSION=1.0.0
PLUGIN_ID=microsoft-adls

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json code-env python-fs-providers

include ../Makefile.inc