var app = angular.module('deepLearningImageTools.scoring', []);

app.controller('scoringRecipeController', function($scope) {


    var retrieveCanUseGPU = function() {

        $scope.callPythonDo({method: "get-info-scoring"}).then(function(data) {
            $scope.canUseGPU = data["can_use_gpu"];
            $scope.finishedLoading = true;
        }, function(data) {
            $scope.canUseGPU = false;
            $scope.finishedLoading = true;
        });
    };

    var initVariable = function(varName, initValue) {
        if ($scope.config[varName] == undefined) {
            $scope.config[varName] = initValue;
        }
    };

    var initVariables = function() {
        initVariable("max_nb_labels", 5);
        initVariable("min_threshold", 0);
        initVariable("gpu_allocation", 0.5);
        initVariable("list_gpu", "0");
    };

    var init = function() {
        $scope.finishedLoading = false;
        initVariables();
        retrieveCanUseGPU();
    };

    init();
});
