(function() {
    var app = angular.module('createDatastore', []);

    "use strict";
    app.controller('CreateDatastoreController', ['$scope', '$http', '$filter', '$log',
        function($scope, $http, $filter, $log) {
            $scope.datastoreToCreate = [];
            $scope.datastoreToCreate['datastoreType'] = null;

            function getDatastoreEndpoint () {
                return '/v1/clusters';
            }


            $scope.createDatastore = function (datastoreToCreate) {
                $scope.datastoreCreatePromise = null;
                $scope.datastoreCreateErrorResponse = null;
                $scope.datastoreCreateSuccessResponse = null;

                var newDatastore = {
                    'redshift_id': datastoreToCreate['datastoreName'],
                    'host': datastoreToCreate['datastoreEndpoint'],
                    'port': datastoreToCreate['datastorePort'],
                    'db_schema': datastoreToCreate['datastoreSchema']
                };

                var endpoint = getDatastoreEndpoint();
                $scope.datastoreCreatePromise = $http.post(endpoint, newDatastore).success(function (data) {
                    $scope.datastoreCreatePromise = null;
                    $scope.datastoreCreateErrorResponse = null;
                    $scope.datastoreCreateSuccessResponse = data;
                }).error(function(data) {
                    $scope.datastoreCreatePromise = null;
                    $scope.datastoreCreateSuccessResponse = null;
                    $scope.datastoreCreateErrorResponse = data;
                });
            };


        }]);

})();
