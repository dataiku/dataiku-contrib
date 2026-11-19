(function() {
    const pluginDesc = window.dkuAppConfig.loadedPlugins.find(d => d.id === 'wms-maps-backgrounds');
    const backgrounds = pluginDesc?.params?.backgrounds ?? [];

    for (const { id, label, url, layerId } of backgrounds) {
        dkuMapBackgrounds.addWMS('wms-maps-backgrounds-' + id, label, 'WMS Plugin', url, layerId);
    }
})();
