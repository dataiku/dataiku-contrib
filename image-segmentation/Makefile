PLUGIN_VERSION=0.0.2
PLUGIN_ID=image-segmentation

all:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip custom-recipes plugin.json python-lib code-env
