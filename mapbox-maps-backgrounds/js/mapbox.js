(function() {
    const pluginDesc = window.dkuAppConfig.loadedPlugins.find(d => d.id === 'mapbox-maps-backgrounds');

    if (!pluginDesc || !pluginDesc.params) {
        console.info("Mapbox Plugin: no settings, bailing out");
        return;
    }

    var pluginSettings = pluginDesc.params;

    var accessToken = pluginSettings.accessToken;

    if (!accessToken) {
        console.info("Mapbox: no access token, bailing out");
        return;
    }

    var allStyles = []

    if (pluginSettings.addDefaultStyles) {
        var defaultStyles =  [
            ['mapbox.satellite', "Mapbox Satellite"],
            ['mapbox.mapbox-streets-v8', "Mapbox Streets v8"],
            ['mapbox.mapbox-streets-v7', "Mapbox Streets v7"],
            ['mapbox.mapbox-streets-v6', "Mapbox Streets v6"],
            ['mapbox.mapbox-streets-v5', "Mapbox Streets v5"],
            ['mapbox.mapbox-terrain-v2', "Mapbox Terrain v2"],
            ['mapbox.mapbox-traffic-v1', "Mapbox Traffic v1"]
        ]
        allStyles = allStyles.concat(defaultStyles)
    }

    if (pluginSettings.additionalStyles) {
        allStyles = allStyles.concat(
                pluginSettings.additionalStyles.split("\n").filter(x => x.length > 0).map(x => [x.split(" ")[0], x.split(" ")[1]])
            )
    }

    allStyles.forEach(function(bg) {
        dkuMapBackgrounds.addMapbox(bg[0], bg[1], accessToken);
    });
})();
