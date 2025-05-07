PLUGIN_VERSION=1.1.2
PLUGIN_ID=gutenberg

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-connectors code-env

all: plugin