// Fill in the form with existing credentials
$("#get-existing-credentials").click(function() {
        
    var d = Object();
    d["api-key"] = dataiku.defaultAPIKey ;
    
    var selectors = [
        "powerbi-username", 
        "powerbi-password",
        "powerbi-client-id",
        "powerbi-client-secret",
        "powerbi-resource",
        "powerbi-grant-type",
        "powerbi-scope"
    ]
       
    $.ajax({
        method     :"GET",
        url        : getWebAppBackendUrl("get-existing-credentials"),
        dataType   : "json",
        contentType: "application/json",
        data       : d
    }).success(function(resp) {
        $("powerbi-form").trigger("reset");        
        $.each(selectors, function( index, selector ) {
            $("#"+selector).val(resp[selector]);
        });
    }).error(function(error) {
        console.log(error);
    });
    
});


// Simply create and display a new Power BI token in the UI (for testing and/or copy-pasting)
$("#display-new-token").click(function() {        
    $.ajax({
        method     : "GET",
        url        : getWebAppBackendUrl("display-new-token"),
        dataType   : "json",
        contentType: "application/json",
        data       : $("#powerbi-form").serialize()
    }).success(function(resp) {
        $("#results").empty();
        $("#results").text(JSON.stringify(resp, null, 2));
    }).error(function(error) {
        console.log(error);
    });
});


// Create a new Power BI token and save settings as DSS Project Variables
$("#save-new-token").click(function() {
    
    var d = Object()
    d["api-key"] = dataiku.defaultAPIKey ;
    $.each($("#powerbi-form").serializeArray(), function(_, kv) {
        d[kv.name] = kv.value;
    });
    d["webapp-url"] = getWebAppBackendUrl("save-new-token")
    
    $.ajax({
        method     :"POST",
        url        : getWebAppBackendUrl("save-new-token"),
        dataType   :"json",
        contentType:"application/json",
        data       : JSON.stringify(d)
    }).success(function(resp) {
        $("#results").empty();
        $("#results").text(JSON.stringify(resp, null, 2));
    }).error(function(error) {
        console.log(error);
    });
    
});


// Simple service to retrieve a token from existing credentials
function get_token() {
    var d = Object()
    d["api-key"] = dataiku.defaultAPIKey ;
    $.ajax({
        method     :"GET",
        url        : getWebAppBackendUrl("get-token"),
        dataType   : "json",
        contentType: "application/json",
        data       : d
    }).success(function(resp) {
        console.info(getWebAppBackendUrl("get-token"));
        console.info(resp);
    }).error(function(error) {
        console.log(error);
    });
}

$("#get-token").click(function() {
    get_token();
});