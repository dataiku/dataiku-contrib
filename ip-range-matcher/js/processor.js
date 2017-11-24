(function(){
    'use strict';

    var injector = angular.element("body").injector();
    var ShakerProcessorsInfo = injector.get("ShakerProcessorsInfo");

    ShakerProcessorsInfo.map["IPRangeMatcher"] = {
        "description" : function(type, params){
            if (!params.ipColumn) return null;
            return "Matches IPs in column <strong>{0}</strong> against specified ranges".format(sanitize(params["ipColumn"]));
        },
        "icon" : "icon-eye-close"
    }

})();
