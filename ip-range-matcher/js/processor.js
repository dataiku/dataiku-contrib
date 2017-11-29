(function() {
    'use strict';

    var injector = angular.element("body").injector();
    var ShakerProcessorsInfo = injector.get("ShakerProcessorsInfo");

    var createDescription = function(params, action) {
        var lines = (params["ranges"].match(/^.+$/mg) || '').length;
        var ranges = lines > 1 ? "one of the " + lines + " ranges" : "the specified range";

        var cols = " from ";
        switch (params["appliesTo"]) {
            case "ALL":
                cols += "all columns";
                break;
            case "PATTERN":
                cols = "";
                break;
            default:
                cols += params["columns"].join(", ");
                break;
        }

        return "<strong>{0}</strong> rows{1} where IPs match {2}".format(sanitize(action), sanitize(cols), sanitize(ranges));
    }

    ShakerProcessorsInfo.map["FlagOnIPRange"] = {
        "description": function(type, params) {
            if (!params["ranges"] || !params["columns"] || params["ranges"].length == 0 || params["columns"].length == 0) return null;

            return createDescription(params, "Flag");
        },
        "icon": "icon-flag"
    }

    ShakerProcessorsInfo.map["FilterOnIPRange"] = {
        "description": function(type, params) {
            if (!params["ranges"] || !params["columns"] || params["ranges"].length == 0 || params["columns"].length == 0) return null;

            return createDescription(params, "Filter");
        },
        "icon": "icon-trash"
    }
})();