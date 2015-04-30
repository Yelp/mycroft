(function() {
  var app = angular.module('viewJobs', ['mycroftService']);

  "use strict";
  app.controller('ViewJobsController', ['$scope', '$http', '$location', '$window', '$log', '$rootScope',
    '$filter', 'commonService',
    function($scope, $http, $location, $window, $log, $rootScope, $filter, commonService) {

    $scope.jobFilter = {};

    // Default to show/hide on page-load
    $scope.columns = {
        'logName': true,
        'schemaVersion': true,
        's3Path': false,
        'contactEmails': false,
        'uuid': false,
        'redshiftEndpoint': true,
        'startDate': true,
        'endDate': true,
        'status': true
    };

    // We'll preserve any errors we get when trying to list jobs
    $scope.listJobsError = false;

    // Initiate a promise for the list of jobs
    $scope.listJobsPromise = $http.get('/v1/jobs').success(function (data) {
      $scope.parsedJobs = parseJobListResponse(data);
      $scope.listJobsPromise = null;
      $scope.listJobsError = false;
    }).error(function (errorData) {
      $scope.listJobsPromise = null;
      $scope.listJobsError = errorData;
    });

    function getRunsEndpoint (){
      return '/v1/runs/';
    }

    function convertToLocalDatetime(datetime_string){
      utcDate = new Date(datetime_string+" UTC");
      localDate = new Date(utcDate.toLocaleString());
      return $filter('date')(localDate, 'yyyy-MM-dd HH:mm:ss');
    }

    /**
     * Get the run status of a job.
     *
     * @param  {int} uuid - the job uuid
     */
    function getRunStatus(uuid) {
      var i;
      var endpoint = getRunsEndpoint() + uuid;

      $scope.runs = [];   // Initialize/clear runs.

      $http.get(endpoint).success(function(response) {
        var runs = $scope.runs = response.runs;

        for (i = 0; i < $scope.runs.length; i++) {
          /* Modify the date format to show it properly. We need to add the
           * character "T" as delimiter for different browsers.
           */
          var temp;
          if (runs[i].load_starttime !== null) {
            temp = convertToLocalDatetime(runs[i].load_starttime).split(" ");
            runs[i].load_starttime = $filter('date')(temp[0], 'yyyy-MM-dd') +
            "T" + $filter('date')(temp[1], 'HH:mm:ss');
          }
          if (runs[i].update_at !== null) {
            temp = convertToLocalDatetime(runs[i].updated_at).split(" ");
            runs[i].updated_at = $filter('date')(temp[0], 'yyyy-MM-dd') +
            "T" + $filter('date')(temp[1], 'HH:mm:ss');
          }
          if (runs[i].et_starttime !== null) {
            temp = convertToLocalDatetime(runs[i].et_starttime).split(" ");
            runs[i].et_starttime = $filter('date')(temp[0], 'yyyy-MM-dd') +
            "T" + $filter('date')(temp[1], 'HH:mm:ss');
          }

          // limit the error length to 50 chars
          if (runs[i].etl_error !== null) {
            $scope.errorLength = 50;
            runs[i].etl_error_truncated = $filter('limitTo')(runs[i].etl_error, $scope.errorLength);
            if (runs[i].etl_error.length > $scope.errorLength) {
              runs[i].etl_error_truncated = runs[i].etl_error_truncated.concat("...");
            }
          }

          // Disable the collapsible division initially
          runs[i].isRowCollapsed = false;
        }
      }).error(function (data) {
        $scope.error = "Connection to server failed";
      });
    }

    /**
     * Get the css template of run status
     *
     * @param  {string} runStatus  - the run status
     */
    $scope.getRunsResultClass = function(runStatus) {
      if (runStatus.etl_status == 'load_success' || runStatus.etl_status == 'et_success') {
        return 'success';
      }
      if (runStatus.etl_status == 'load_error' || runStatus.etl_status == 'et_error') {
        return 'danger';
      }
    };

    /**
     * Toggle the collapse of a run status to show the error. It is only expandable
     * for the failed run.
     *
     * @param  {int} index - the row index
     */
    $scope.selectRow = function (run) {
      if (run.etl_status !== 'et_error' && run.etl_status !== 'load_error') {
        return;
      }

      run.isRowCollapsed = !run.isRowCollapsed;
    };

    /**
     * Parses a list of jobs out of a /v1/jobs GET API response
     *
     * @param  {job response object} jobResponse - from the /v1/jobs endpoint
     * @return {array} An array of jobs
     */
    function parseJobListResponse(jobResponse){
      var jobs = jobResponse.jobs;
      var job = null;

      var parsedJobs = jobs.map(function (job){
        return {
          'logName': job.log_name,
          'schemaVersion': job.log_schema_version,
          's3Path': job.s3_path,
          'contactEmails': job.contact_emails,
          'uuid': job.uuid,
          'redshiftEndpoint': job.redshift_id,
          'startDate': job.start_date,
          'endDate': job.end_date,
          'et_status': job.et_status
        };
      });

      for (var i = 0; i < jobs.length; i++) {
        job = jobs[i];
        var parsedJob = parsedJobs[i];

        // Extract job status from et_status
        parsedJobs[i]['status'] = job['et_status'];

        // Show proper intermediate statuses. Three cases:
        // a) if cancel_requested is set, if status is not in cancelled yet, it is in cancelling
        // b) if pause_requested is set, if status is not in paused, cancelled or complete, it
        // is in pausing.
        // c) if delete_requested is set, then job is not yet deleted, show 'deleting'
        if (job['delete_requested'] == '1' ||
                ['cancelled', 'complete'].indexOf(parsedJobs[i]['status']) >= 0) {
            parsedJobs[i]['showDelete'] = true;
        } else {
            parsedJobs[i]['showDelete'] = false;
        }

        if (job['delete_requested'] == '1') {
          parsedJobs[i]['status'] = 'deleting';
        } else if (job['cancel_requested'] == '1' &&
            ['cancelled', 'complete'].indexOf(parsedJobs[i]['status']) < 0) {
          parsedJobs[i]['status'] = 'cancelling';
        } else if (job['pause_requested'] == '1' &&
            ['paused', 'cancelled', 'complete'].indexOf(parsedJobs[i]['status']) < 0) {
          parsedJobs[i]['status'] = 'pausing';
        } else if (job['pause_requested'] == '0' && parsedJobs[i]['status'] == 'paused') {
          parsedJobs[i]['status'] = 'resuming';
        }
      }

      // decide whether to enable the action links according to job status
      for (i = 0; i < parsedJobs.length; i++) {
        job = jobs[i];
        // enable pause link if job is is null, scheduled, error and success
        // and pause_requested is not set.
        if (['null', 'scheduled', 'error', 'running', 'success'].indexOf(parsedJobs[i]['status'])>-1 &&
              parsedJobs[i]['pauseRequested'] != '1') {
          parsedJobs[i]['pauseLinkEnable'] = "enabled";
        } else {
          parsedJobs[i]['pauseLinkEnable'] = "disabled";
        }

        // enable resume link only job is in following combination of states
        if (parsedJobs[i]['et_status'] == 'paused') {
          parsedJobs[i]['resumeLinkEnable'] = "enabled";
        } else {
          parsedJobs[i]['resumeLinkEnable'] = "disabled";
        }
      }

      return parsedJobs;
    }

    // initial sorting order for job status table
    $scope.sortJobKeys = {
      column: ['logName', 'schemaVersion'],
      descending: false
    };

    // initial sorting order for run status table
    $scope.sortRunKeys = {
      column: ['data_date'],
      descending: true
    };

    $scope.changeSorting = function(sortKey, column) {
      commonService.sortColumn(sortKey, column);
    };

    /**
    * Check if the delete button should be disabled or not.
    *
    * @param  {job response object} job - current job status
    * @return {boolean} - whether to disable the button or not.
    */
    $scope.disableDeleteButton = function(job) {
      return job.status == 'deleting' ? true : false;
    };

    /**
    * Check if the cancel button should be disabled or not.
    *
    * @param  {job response object} job - current job status
    * @return {boolean} - whether to disable the button or not.
    */
    $scope.disableCancelButton = function(job) {
      if (job.status == 'complete' || job.status == 'cancelling' || job.status == 'cancelled') {
        return true;
      }
      return false;
    };

    /**
    * Cache the job of corresponding deletion.
    *
    * @param  {job response object} job - current job status
    */
    $scope.initDeleteRequest = function(job) {
      $scope.deleteJob = job;
    };

    /**
    * Perform the cancellation after user confirms it.
    */
    $scope.execDeleteRequest = function() {
      var job = $scope.deleteJob;
      var jobInput = {
        'log_name': job.logName,
        'log_schema_version': job.schemaVersion,
        'redshift_id': job.redshiftEndpoint,
        'start_date': job.startDate,
        'end_date': job.endDate,
        'delete_requested': true
      };

      $http.put('/v1/jobs/job', jobInput).success(function (data) {
        // Once PUT is successful, change the status to deleting for better interaction.
        $scope.deleteJob['status'] = 'deleting';
      });
    };

    /**
    * Cache the job of corresponding cancellation.
    *
    * @param  {job response object} job - current job status
    */
    $scope.initCancelRequest = function(job) {
      $scope.cancelJob = job;
    };

    /**
    * Perform the cancellation after user confirms it.
    */
    $scope.execCancelRequest = function() {
      var job = $scope.cancelJob;
      var jobInput = {
        'log_name': job.logName,
        'log_schema_version': job.schemaVersion,
        'redshift_id': job.redshiftEndpoint,
        'start_date': job.startDate,
        'end_date': job.endDate,
        'cancel_requested': true
      };

      $http.put('/v1/jobs/job', jobInput).success(function (data) {
        // Once PUT is successful, change the status to cancelling for better interaction.
        $scope.cancelJob['status'] = 'cancelling';
      });
    };

    $scope.execPauseRequest = function(job) {
      var jobInput = {
        'log_name': job.logName,
        'log_schema_version': job.schemaVersion,
        'redshift_id': job.redshiftEndpoint,
        'start_date': job.startDate,
        'end_date': job.endDate,
        'pause_requested': true
      };

      $http.put('/v1/jobs/job', jobInput).success(function (data) {
        // Once PUT is successful, change the status to pausing for UI.
        job['status'] = 'pausing';
        job['pauseLinkEnable'] = 'disabled';
      });
    };

    $scope.execResumeRequest = function(job) {
      var jobInput = {
        'log_name': job.logName,
        'log_schema_version': job.schemaVersion,
        'redshift_id': job.redshiftEndpoint,
        'start_date': job.startDate,
        'end_date': job.endDate,
        'pause_requested': false
      };

      $http.put('/v1/jobs/job', jobInput).success(function (data) {
        job['status'] = 'resuming';
        job['resumeLinkEnable'] = 'disabled';
      });
    };

    // Sets up two way binding between URL arguments and the jobFilter.
    // We want the URL to reflect the filter box.
    $scope.$watch('jobFilter', function (newJobFilter, oldJobFilter) {
      if (oldJobFilter === newJobFilter){
        // newJobFilter should be the same as oldJobFilter only one time,
        // at initialization. In this case, we should not modify the URL -
        // we want to initialize jobFilter from the URL, not the other way around.
        // This allows people to open a link with a pre-filled search box.
        return;
      }
      else {
        if (newJobFilter.logName || newJobFilter.schemaVersion || newJobFilter.uuid || newJobFilter.et_status) {
          _(newJobFilter).extend(_($location.search()).omit('logName', 'schemaVersion', 'uuid', 'et_status'));
          $location.search(newJobFilter);
        } else {
          // Clean up the URL if there is no filter text
          $location.search({});
        }
    }
    }, true);

    // Watch the URL change to update the job filter and extend the run details.
    $scope.$watch(function () { return $location.search(); }, function (newParams) {
      // On initialization, the model and the URL might not match. In
      // that case, we should init the model from the URL.
      var newJobFilter = _(newParams).pick('logName', 'schemaVersion', 'uuid', 'et_status');
      if (newJobFilter.logName && !angular.isArray(newJobFilter.logName)) {
        newJobFilter.logName = [newJobFilter.logName];
      }
      if (newJobFilter.schemaVersion && !angular.isArray(newJobFilter.schemaVersion)) {
        newJobFilter.schemaVersion = [newJobFilter.schemaVersion];
      }
      if (newJobFilter.et_status && !angular.isArray(newJobFilter.et_status)) {
        newJobFilter.et_status = [newJobFilter.et_status];
      }
      if ($scope.jobFilter !== newJobFilter){
        $scope.jobFilter = {
          logName: newJobFilter.logName,
          schemaVersion: newJobFilter.schemaVersion,
          uuid: newJobFilter.uuid,
          et_status: newJobFilter.et_status
        };

        if (newParams.uuid !== undefined && newParams.jobDetails == "true") {
          getRunStatus(newParams.uuid);
          $scope.showJobDetails = true;
        } else {
          $scope.showJobDetails = false;
        }
      }
    });
  } ]);

  // Custom filter for multiple key words. Used to filter jobs.
  app.filter('filterMultiple',['$filter',function ($filter) {
    //Items is the search items set, currentFilter is the dict for key-val filter.
    return function (items, currentFilter) {
      var filterObject = {
        //filterData is default to set original result set.
        filteredData: items,

        applyFilter : function(filterAttributeName, filterAttributeValue){
          var tmpfilteredData = [];
          if (filterAttributeValue){
            //temporary search dict
            var tmpfilteredObj = {};

            //Create filtered data. For single item search, transfer it to an array first.
            if (!angular.isArray(filterAttributeValue)) {
                filterAttributeValue = [filterAttributeValue];
            }
            //"ng-list" directive in view-jobs.html will automatically split string with commas into a an array split on the comma
            if(filterAttributeValue.length == 0) {
              tmpfilteredData = this.filteredData;
            } else {
              for (var i = 0; i < filterAttributeValue.length; i++) {
                if (angular.isDefined(filterAttributeValue[i])) {
                  tmpfilteredObj[filterAttributeName] = filterAttributeValue[i];
                  tmpfilteredData = tmpfilteredData.concat($filter('filter')(this.filteredData, tmpfilteredObj));
                }
              }
            }
            this.filteredData = tmpfilteredData;
          }
        }
      };
      //if current filter object is set, apply filter
      if (currentFilter){
        angular.forEach(currentFilter,function(searchContent,searchKey){
          //applying filter to each attribute of the filter object
          filterObject.applyFilter(searchKey, searchContent);
        });
      }
      return filterObject.filteredData;
    }
  }]);
} )();
