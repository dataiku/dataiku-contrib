PLUGIN_VERSION=0.1.0
PLUGIN_ID=wikipedia-pageviews

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip --exclude "*.pyc" -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip custom-recipes plugin.json python-connectors python-lib code-env