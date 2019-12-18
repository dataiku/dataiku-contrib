// Access the parameters that end-users filled in using webapp config
// For example, for a parameter called "input_dataset"
// input_dataset = dataiku.getWebAppConfig()['input_dataset']

$.getJSON(getWebAppBackendUrl('/first_api_call'), function(data) {
    $('#algorithmId').text(data["data"]["agent_name"])
    $('#timestampId').text((data["data"]["trainingdate"]).split('.')[0])
    $('#environmentId').text(data["data"]["environmentName"])
    $('#averageScoreId').text(data["data"]["average_score"])
    $('#policyId').text(data["data"]["policy"])
    $('#learningRateId').text(data["data"]["lr"])
    $('#gammaId').text(data["data"]["gamma"])
    //$('#video_pathId').attr('src', 'data:video/mp4;base64,' + data.video); 
    $('#nbTrainingEpisodesId').text(data["data"]["total_timesteps"], data["data"]["total_episodes"])
});
