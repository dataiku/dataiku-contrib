PLUGIN_VERSION=0.0.2
PLUGIN_ID=wikipedia-pageviews

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip pcustom-recipes plugin.json python-connectors python-lib