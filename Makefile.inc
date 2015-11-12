# Generic rules to help plugins development


install-in-dss: plugin
	${DIP_HOME}/bin/dku install-plugin dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip
	${DIP_HOME}/bin/dss restart backend

reinstall-in-dss: plugin
	${DIP_HOME}/bin/dku install-plugin dist/dss-plugin-${PLUGIN_ID}-${PLUGIN_VERSION}.zip -u
	${DIP_HOME}/bin/dss restart backend