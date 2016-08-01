(function(){
    'use strict';

    var injector = angular.element("body").injector();
    var ShakerProcessorsInfo = injector.get("ShakerProcessorsInfo");

    ShakerProcessorsInfo.map["AnonymizerProcessor"] = {
        "description" :function(type, params){
            if (!params.inputColumn) return null;
            return "Anonymize data in column <strong>{0}</strong>".format(sanitize(params["inputColumn"]));
        },
        "icon" : "icon-eye-close"
    }

})();