all:
	ant clean
	ant
	zip -r dss-plugin-ip-range-matcher-0.0.1.zip lib plugin.json

reinstall-in-dss: all
	${DIP_HOME}/bin/dku install-plugin dss-plugin-ip-range-matcher-0.0.1.zip -u
	${DIP_HOME}/bin/dss restart backend
