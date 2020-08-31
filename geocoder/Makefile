PLUGIN_VERSION=1.1.1
PLUGIN_ID=geocoder

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip code-env custom-recipes plugin.json python-lib
