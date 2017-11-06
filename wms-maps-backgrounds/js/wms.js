(function() {
    const pluginSettings = window.dkuAppConfig.loadedPlugins.find(d => d.id === 'wms-maps-backgrounds').params;

    var wmsResources = pluginSettings.wmsResources;

    if (!wmsResources) {
        console.info("WMS Plugin: no config, bailing out");
        return;
    }

    var wmsList = wmsResources.split("\n").filter(x => x.length > 0).map(x => [x.split(" ")[0], x.split(" ")[1], x.split(" ")[2]]);

    for (let i = 0; i < wmsList.length; i++) {
        dkuMapBackgrounds.addWMS(
            'mws-maps-backgrounds-plugin-' + i,
            wmsList[i][2],
            'WMS Plugin',
            wmsList[i][0],
            wmsList[i][1]
        );
    }
})();
