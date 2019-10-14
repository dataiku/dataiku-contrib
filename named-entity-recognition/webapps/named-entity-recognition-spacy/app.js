let webAppConfig = dataiku.getWebAppConfig();

$('#main').on('submit', function(e) {
    e.preventDefault();
    run_NER();
});

function run_NER() {
    var input = $("#input_text").val();

    $.getJSON(getWebAppBackendUrl('run_NER'), {"input": input})
    .done(function(data) {
        $("#results").html(data);
    })
    .fail(function() {
        alert("Error or no response from the backend. Did the backend start?");
    });
}

run_NER();

