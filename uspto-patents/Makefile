PLUGIN_ID=uspto-patents
PLUGIN_VERSION=0.0.2

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip code-env plugin.json python-connectors

include ../Makefile.inc
