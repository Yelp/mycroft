(function() {
    var mycroftApp = angular.module('mycroftApp', [
        'ngRoute',
        'ui.bootstrap',
        'navbar',
        'viewSchema',
        'uploadSchema',
        'createJobs',
        'viewJobs',
        'createDatastore'
    ]);

    "use strict";
    mycroftApp.config(['$routeProvider',
        function($routeProvider) {
            $routeProvider.
            when('/schema/view', {
                templateUrl: 'partials/view-schema.html',
                controller: 'ViewSchemaController',
                // The view schema page has a filter box which updates the url
                // as you type; it's really annoying if the page refreshes for every
                // typed character.
                reloadOnSearch: false
            }).
            when('/schema/upload', {
                templateUrl: 'partials/upload-schema.html',
                controller: 'UploadSchemaController'
            }).
            when('/jobs/create', {
                templateUrl: 'partials/create-jobs.html',
                controller: 'CreateJobsController'
            }).
            when('/jobs/view', {
                templateUrl: 'partials/view-jobs.html',
                controller: 'ViewJobsController',
                // Refer to comment on the /schema/view block
                reloadOnSearch: false
            }).
            when('/datastores/register', {
                templateUrl: 'partials/create-datastores.html',
                controller: 'CreateDatastoreController'
            }).
            otherwise({
                redirectTo: '/jobs/create'
            });
        }
    ]);


    /**
     * attach the .equals method to Array's prototypey
     */
    Array.prototype.equals = function (array) {
        // if the other array is a falsy value, return
        if (!array)
            return false;

        if (this.length != array.length)
            return false;

        for (var i = 0; i < this.length; i++) {
            // Check if we have nested arrays
            if (this[i] instanceof Array && array[i] instanceof Array) {
                // recurse into the nested arrays
                if (!this[i].equals(array[i]))
                    return false;
            } else if (this[i] != array[i]) {
                // Warning - two different object instances will never be equal: {x:20} != {x:20}
                return false;
            }
        }
        return true;
    };
} )();
