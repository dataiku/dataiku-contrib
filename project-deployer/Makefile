PLUGIN_VERSION=1.0.2
PLUGIN_ID=project-deployer

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-runnables

include ../Makefile.inc
