(function() {
    var app = angular.module('createJobs', []);

    "use strict";
    app.controller('CreateJobsController', ['$scope', '$http', '$filter', '$log', '$location',
        function($scope, $http, $filter, $log, $location) {
            // Keep track of today's date
            $scope.today = new Date();

            // Initialize jobToCreate. The key data structure for job posting.
            $scope.jobToCreate = [];
            $scope.jobToCreate[redshiftEndpoint] = null;
            $scope.jobToCreate[logName] = null;
            $scope.jobToCreate.endDate = null;

            function getJobsEndpoint () {
                return '/v1/jobs';
            }

            function getClustersEndpoint() {
                return '/v1/clusters';
            }

            function getSourceLogsEndpoint() {
                return '/v1/log_source';
            }

            function clusterInfo() {
                this.redshift_id;
                this.group;
            }

            $scope.clusterListPromise = $http.get(getClustersEndpoint()).success(function (data) {
                $scope.clusterListPromise = null;
                $scope.clusterList = [];
                var clusters = data.clusters;

                for (var i = 0; i < clusters.length; i++) {
                    var temp = new clusterInfo();
                    temp.redshift_id = clusters[i].redshift_id;
                    if (clusters[i].groups === null) {
                        temp.group = "null";
                    } else {
                        temp.group = "";
                        for (var j = 0; j < clusters[i].groups.length - 1; j++) {
                            temp.group = temp.group.concat("'"+clusters[i].groups[j]+"', ");
                        }
                        temp.group = temp.group.concat("'"+clusters[i].groups[j]+"'");
                    }
                    $scope.clusterList[$scope.clusterList.length] = temp;
                }
            }).error(function(errorData) {
                $scope.clusterListPromise = null;
                $scope.clusterListError = errorData;
            });

            // Initiate a promise for the schemas object.
            $scope.schemaListPromise = $http.get('/v1/schema').success(function (data) {
                $scope.schemaList = unrollSchemas(data);
                $scope.schemaListPromise = null;
            }).error(function (errorData) {
                $scope.schemaListPromise = null;
                $scope.schemaListError = errorData;
            });

            /**
             * Given a schema response object (from the /v1/schema end-point), this
             * function will extract the contained the object into a dict of schemas.
             * Manage the dict in an object.
             *
             * @param {schema response object} schemaResponse - from the /v1/schema end-point
             * @return {object} unrolledSchemas - an object of {logName: [versions]}
             */
            function unrollSchemas(schemaResponse) {
                if (!schemaResponse){
                    return null;
                }
                var schemaDict = {};

                schemaResponse.schemas.forEach(function (schema) {
                    schemaDict[String(schema.log_name)] = schema.versions;
                });
                return schemaDict;
            }

            /**
             * Marks a datepicker widget as opened.
             *
             * Code taken from: http://angular-ui.github.io/bootstrap/
             *
             * @param  {string} openedVar - name of the variable that's keeping track of the open
             *      state for this particular datepicker.
             */
            $scope.openDatePicker = function($event, openedVar) {
                $event.preventDefault();
                $event.stopPropagation();

                $scope[openedVar] = true;
            };


            /**
             * Converts a Date object into the yyyy-MM-dd string format that the API expects
             *
             * @param {Date} date_obj - date object to convert
             * @return {string} - string version of the input date, in the desired format
             */
            function formatDateForAPI(date_obj){
                return $filter('date')(date_obj, 'yyyy-MM-dd');
            }

            /**
             * Crates an array by breaking up the values in a comma-separated string.
             *
             * Any whitespace between the commas and values will be stripped.
             *
             * @param {string} csv_string - comma-separated string to break up.
             * @return {array} - array of values from csv_string
             */
            function parseCommaSeparatedValues(csv_string){
                var value_array = csv_string.split(',');
                var trimmed_value_array = value_array.map(function (value) {
                    return value.trim();
                });
                return trimmed_value_array;
            }

            /**
            * Set error response with error message
            *
            * @param (string) error_msg - error message of job create
            */
            function setErrorResponse(error_msg){
                $scope.jobCreatePromise = null;
                $scope.jobCreateSuccessResponse = null;
                $scope.jobCreateErrorResponse = error_msg;
                return;
            }

            /**
             * Takes a job and POSTs it to the /jobs end-point.
             *
             * @param  {job object} job - object with the following attributes:
             *      redshiftEndpoint {string} - Redshift URL endpoint for the job
             *      contactEmails {string} - comma separated list of emails addresses that should be
             *          contacted for updates about the job
             *      logName {string} - schema log name that the job should use
             *      schemaVersion {string} - log schema version for the job to use
             *      s3Path {string} - S3 directory URL for the log that the job will process
             *      startDate {Date} - job's start date
             *      endDate {Date} - optional, job's end date
             *      additionalArguments {json} - optional arguments passed through to workers
             *
             * There is no return value - the function creates a promise.
             */
            $scope.createJob = function (job){

                $scope.jobCreatePromise = null;
                $scope.jobCreateErrorResponse = null;
                $scope.jobCreateSuccessResponse = null;

                var endpoint = getJobsEndpoint();
                var uri = null;
                var logFormat = null;

                // Check logMetaData is selected from type-ahead or entered by user manually.
                // If it is entered by user, we interpret it as a uri path.
                if (job.logMetaData.hasOwnProperty('uri')) {
                    uri = job.logMetaData.uri;
                    logFormat = job.logMetaData.format;
                } else {
                    uri = job.logMetaData;
                }

                // Initialize with the required keys
                var jobCreateBody = {
                    'redshift_id': job.redshiftEndpoint,
                    'log_name': job.logName,
                    'log_schema_version': job.schemaVersion,
                    's3_path': uri,
                    'log_format': logFormat,
                    'start_date': formatDateForAPI(job.startDate),
                    'contact_emails': parseCommaSeparatedValues(job.contactEmails),
                    'additional_arguments': job.additionalArguments
                };

                // Add optional keys only if they exist, since an empty key and a missing
                // key could be semantically different.
                if (jobCreateBody['start_date'] === undefined){
                    setErrorResponse("Invalid start date");
                    return;
                }
                if (job.endDate === undefined){
                    setErrorResponse("Invalid end date");
                    return;
                }

                if (job.endDate !== null){
                    if (job.startDate > job.endDate){
                        setErrorResponse("start date should not be greater than end date");
                        return;
                    }
                    jobCreateBody['end_date'] = formatDateForAPI(job.endDate);
                }

                if (jobCreateBody['additional_arguments'] !== undefined && jobCreateBody['additional_arguments'].length > 0){
                    try{
                        angular.fromJson(jobCreateBody['additional_arguments']);
                    }
                    catch(err) {
                        setErrorResponse("Invalid additional arguments. Additional arguments should be json format");
                        return;
                    }
                }
                $scope.jobCreatePromise  = $http.post(endpoint, jobCreateBody).success(function (data) {
                    $scope.jobCreatePromise = null;
                    $scope.jobCreateErrorResponse = null;
                    $scope.jobCreateSuccessResponse = data;

                    // Jump to job view page if job is created successfullly
                    $location.path('/jobs/view').search('uuid', data.post_accepted.uuid);
                }).error(function (data) {
                    setErrorResponse(data);
                });
            };

            $scope.getSuggestionList = function(val) {
                return $http.post(getSourceLogsEndpoint()+'/search', {'keyword': val}).then(function(response) {
                    return response.data.logs.map(function(item){
                        return item;
                    });
                });
            };

            $scope.updateJobRedshiftEndpoint = function(cluster) {
                $scope.jobToCreate.redshiftEndpoint = cluster.redshift_id;
            };

            $scope.updateJobLogName = function(schema) {
                $scope.jobToCreate.logName = schema;
            };

            $scope.updateJobSchemaVersion = function(version) {
                $scope.jobToCreate.schemaVersion = version;
            };

            /**
             * Monitor the input of schema. Fill out schema version and s3 path
             * according to schema.
             */
            $scope.$watch(function() {
                    return $scope.jobToCreate.logName;
                },
                function() {
                    if (typeof $scope.jobToCreate.logName !== 'undefined') {
                        var log_name = $scope.jobToCreate.logName;
                        var schemaList = $scope.schemaList;

                        // Check if the default version exists or not.
                        if (log_name in schemaList) {
                            if (schemaList[log_name].indexOf('default') >= 0)
                                $scope.jobToCreate.schemaVersion = 'default';
                            else
                                $scope.jobToCreate.schemaVersion = null;
                        }
                    } else {
                        // clear the version if schema field is entirely deleted
                        $scope.jobToCreate.schemaVersion = null;
                    }
                }
            );
        }]);
})();
