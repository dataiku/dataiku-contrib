PLUGIN_VERSION=0.0.3
PLUGIN_ID=tableau-hyper-export

plugin:
	cat plugin.json|json_pp > /dev/null
	rm -rf dist
	mkdir dist
	zip -r dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip plugin.json python-lib code-env python-exporters install.sh requirements.json

include ../Makefile.inc
