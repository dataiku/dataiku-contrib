// Access the parameters that end-users filled in using webapp config
// For example, for a parameter called "input_dataset"
// input_dataset = dataiku.getWebAppConfig()['input_dataset']

$.getJSON(getWebAppBackendUrl('/first_api_call'), function(data) {
    $('#algorithmId').text(data["data"]["agent"])
    $('#timestampId').text(data["data"]["training_date"])
    $('#environmentId').text(data["data"]["name"])
    $('#nbTrainingEpisodesId').text(data["data"]["num_episodes"])
    $('#averageScoreId').text(data["data"]["average_score"])
    $('#policyId').text(data["data"]["policy"])
    $('#learningRateId').text(data["data"]["lr"])
    $('#gammaId').text(data["data"]["gamma"])
    $('#video_pathId').attr('src', 'data:video/mp4;base64,' + data.video);    
});

