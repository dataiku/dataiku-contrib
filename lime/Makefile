PLUGIN_VERSION=0.0.5
PLUGIN_ID=lime

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json custom-recipes python-lib

clean:
	rm -rf dist
