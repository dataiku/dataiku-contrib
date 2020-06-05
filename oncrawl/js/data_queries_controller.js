var app = angular.module('oncrawl-data_queries.module', []);

app.controller('oncrawl_data_queries', function($scope) {
   
    //Behave.js is a lightweight library for adding IDE style behaviors to plain text areas, making it much more enjoyable to write code in.
    var editor = new Behave({
        textarea: document.getElementById('oql')
    });    
    
    //init default vars
    $scope.api_error = null
    $scope.oql_error = null

    if(!$scope.config.date_kind)
    {
        $scope.config.date_kind = 'relative';
    }
    $scope.toggle_date = false;
    if($scope.config.date_kind == 'absolute')
        $scope.toggle_date = true;
    
    if(!$scope.config.date_filter_time_cursor)
    {
        $scope.config.date_filter_time_cursor = 'current';
        $scope.config.date_filter_unit = 'month';
        $scope.config.date_filter_include_today = true;
        $scope.config.date_filter_type = true;
        
    }
    if(!$scope.config.date_filter_num_unit)
    {
        $scope.config.date_filter_num_unit = 1;
    }
    if(!$scope.config.data_action)
    {
        $scope.config.data_action = 'aggs';
    }
    $scope.toggle_action = false;
    if($scope.config.data_action == 'export')
        $scope.toggle_action = true;
    
    if(!$scope.config.index)
    {
        $scope.config.index = 'pages';
    }

    $scope.selectDefaultCrawls = function()
    {
        $scope.config.crawls_id = selectDefaultCrawls($scope);        
    }
    $scope.$watchGroup(['date_start_yyyy_mm_dd', 'date_end_yyyy_mm_dd'], updateDatesRange);
    function updateDatesRange(o, n)
    {
        if (!$scope.date_start_yyyy_mm_dd && !$scope.date_end_yyyy_mm_dd)
            return 
            
        $scope.config.oql = build_oql($scope);
        if($scope.config.index != 'logs')
        {
            $scope.get_crawls();
        }
    }
    $scope.build_date_range = function()
    {
        $scope.config.date_kind ='relative'
        if($scope.toggle_date)
        {
            $scope.config.date_kind = 'absolute'
        }
        
        $scope.date_start_yyyy_mm_dd = "";
        $scope.date_end_yyyy_mm_dd = "";
        
        
        if($scope.config.date_kind == 'absolute')
        {
            if(!$scope.config.override_date_start_yyyy_mm_dd || !$scope.config.override_date_end_yyyy_mm_dd)
            {
                return;
            }
        }
        if($scope.config.date_kind == 'relative')
        {
            if(!$scope.config.date_filter_num_unit)
            {
                return;
            }
        }
        
        $scope.callPythonDo({'method': 'build_date_range'
                            }).then(function(response) {
            try
            {
                $scope.date_start_yyyy_mm_dd = response.start
                $scope.date_end_yyyy_mm_dd = response.end
            }
            catch(e) {
                $scope.api_error = response.error
            }

        }, function(response) {
            $scope.api_error = "Unexpected error occurred"
        });
    }
    $scope.build_date_range();
    
    
    $scope.build_oql = function(reset=false)
    {
        $scope.config.data_action ='aggs'
        if($scope.toggle_action)
        {
            $scope.config.data_action = 'export'
        }
        
        $scope.config.oql = build_oql($scope, reset)
    }   
    $scope.check_oql = function()
    {
        $scope.oql_error = null;
        $scope.config.oql = document.getElementById('oql').value
        try
        {
            if($scope.config.oql)
            {
                JSON.parse($scope.config.oql)
            }

            //build oql if empty and add default required missing fields
            $scope.config.oql = prettyPrint(build_oql($scope))
            document.getElementById('oql').value = $scope.config.oql
        }
        catch(e)
        {
            $scope.oql_error = e;
        }
    }
    

    if($scope.config.list_projects_id_name)
    {
        $scope.num_projects = Object.keys($scope.config.list_projects_id_name).length;
    }

    if($scope.config.list_configs_crawls)
    {
        $scope.num_configs = Object.keys($scope.config.list_configs_crawls).length;
    }

    
    $scope.get_projects = function()
    {
        
        $scope.callPythonDo({'method': 'get_projects',
                             'offset': $scope.config.projects_filter_offset || 0,
                             'limit': $scope.config.projects_filter_limit || null,
                             'sort': $scope.config.projects_filter_sort || 'name:asc'
                            }).then(function(response) {
            try
            {

                $scope.api_error = null
                
                $scope.config.list_projects_id_name = response.projects;

                $scope.num_projects = Object.keys($scope.config.list_projects_id_name).length;
                
                if(Object.keys(response.projects).length > 1 && !$scope.config.projects_id)
                {
                    $scope.config.projects_id = 'all';
                }
  
                $scope.get_crawls();
                    
            }
            catch(e) 
            {
                $scope.api_error = response.error
            }
        },function(response) {
            $scope.api_error = "Unexpected error occurred"
        });
    }
    
    
    $scope.get_crawls = function()
    {
        
        if(!$scope.config.projects_id)
        {
            return;
        }

        if(!$scope.date_start_yyyy_mm_dd || !$scope.date_end_yyyy_mm_dd)
        {
            return;
        }
        
        if($scope.config.index == 'logs')
        {
            return;
        }
              
        $scope.callPythonDo({'method': 'get_crawls', 
                             'projects_id': $scope.config.projects_id,
                             'date_start_yyyy_mm_dd' : $scope.date_start_yyyy_mm_dd,
                             'date_end_yyyy_mm_dd' : $scope.date_end_yyyy_mm_dd,
                             'index': $scope.config.index
                            }).then(function(response) {
            try
            {

                $scope.config.list_configs_crawls = response.configs;
                $scope.config.list_crawls_project = response.crawls;
                $scope.num_configs = Object.keys($scope.config.list_configs_crawls).length;
                if(!$scope.config.crawl_config)
                {
                    $scope.config.crawl_config = Object.keys(response.configs)[0];
                }

                $scope.selectDefaultCrawls();
                                
            }
            catch(e) {
                $scope.api_error = response.error
            }

        }, function(response) {
            $scope.api_error = "Unexpected error occurred "+response
        });
        
    }
    
});