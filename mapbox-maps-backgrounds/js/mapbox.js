(function() {
    const pluginSettings = window.dkuAppConfig.loadedPlugins.find(d => d.id === 'mapbox-maps-backgrounds').params;

    var accessToken = pluginSettings.accessToken;

    if (!accessToken) {
        console.info("Mapbox: no access token, bailing out");
        return;
    }

    var allStyles = []

    if (pluginSettings.addDefaultStyles) {
        var defaultStyles =  [
            ['mapbox.streets', "Mapbox streets"],
            ['mapbox.light', "Mapbox Light"],
            ['mapbox.dark', "Mapbox Dark"],
            ['mapbox.satellite', "Mapbox Satellite"],
            ['mapbox.streets-satellite', "Mapbox Satellite+Streets"],
            ['mapbox.wheatpaste', "Mapbox Wheatpaste"],
            ['mapbox.streets-basic', "Mapbox Streets Basic"],
            ['mapbox.comic', "Mapbox Comic"],
            ['mapbox.outdoors', "Mapbox Outdoors"],
            ['mapbox.run-bike-hike', "Mapbox Run/Bike/Hike"],
            ['mapbox.pencil', "Mapbox Pencil"],
            ['mapbox.pirates', "Mapbox Pirates"],
            ['mapbox.emerald', "Mapbox Emerald"],
            ['mapbox.high-contrast', "Mapbox High Contrast"]
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
