(function() {
    const pluginDesc = window.dkuAppConfig.loadedPlugins.find(d => d.id === 'wms-maps-backgrounds');
    
    DataikuAPI.plugins.get("wms-maps-backgrounds").success(function(data) {
        if (!data || !data.settings) {
            console.info("WMS Plugin: no settings, bailing out");
            return;
        }
        var wmsList = [];
        if (data.settings.presets && data.settings.presets.length > 0) {
            // new style: with parameter sets
            console.info("Registering WMS map backgrounds from presets");
            wmsList = data.settings.presets.map(p => [p.config.url, p.config.layerId, p.name]);
        } else if (data.settings.config && data.settings.config.wmsResources && data.settings.config.wmsResources.length > 0) {
            // old style, everything in a textarea
            console.info("Registering WMS map backgrounds from parameters");
            var wmsResources = data.settings.config.wmsResources;
            wmsList = wmsResources.split("\n").filter(x => x.length > 0).map(x => [x.split(" ")[0], x.split(" ")[1], x.split(" ")[2]]);
        } else {
            console.info("No settings for registering WMS map backgrounds");
        }

        for (let i = 0; i < wmsList.length; i++) {
            dkuMapBackgrounds.addWMS(
                'mws-maps-backgrounds-plugin-' + i,
                wmsList[i][2],
                'WMS Plugin',
                wmsList[i][0],
                wmsList[i][1]
            );
        }
    }).error(function(a, b, c) {
        console.error("Failed to get WMS plugin settings", getErrorDetails(a, b, c));
    });

})();
