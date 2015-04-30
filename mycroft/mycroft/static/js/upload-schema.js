(function() {
    "use strict";
    var app = angular.module('uploadSchema', []);

    app.controller('UploadSchemaController', ['$scope', '$http',
        function($scope, $http) {

        $scope.schemaToUpload = {};

        function getSchemaEndpoint (schema){
            return '/v1/schema/' + schema.logName + '/' + schema.version;
        }

        /**
         * Takes a schema and POSTs it to the schema end-point.
         * @param  {schema object} schema - object with the following attributes:
         *      logName {string} - name of the schema's log
         *      version {string} - schema version
         *      content {string} - schema file contents, loaded as a string
         *
         * There is no return value - the function creates a promise.
         */
        $scope.uploadSchema = function (schema){

            $scope.schemaUploadPromise = null;
            $scope.schemaUploadErrorResponse = null;
            $scope.schemaUploadSuccessResponse = null;

            var endpoint = getSchemaEndpoint(schema);
            $scope.schemaUploadPromise  = $http.post(endpoint, schema.content).success(function (data) {
                // TODO: check the response bytes_written to confirm that it was correct
                $scope.schemaUploadPromise = null;
                $scope.schemaUploadErrorResponse = null;
                $scope.schemaUploadSuccessResponse = data;
            }).error(function (data) {
                $scope.schemaUploadPromise = null;
                $scope.schemaUploadSuccessResponse = null;
                $scope.schemaUploadErrorResponse = data;
            });
        };

        /**
         * Generate a link to the "View schema" page of the dashboard, with pre-filled
         * information for a particular schema.
         *
         * @param  {schema object} schema - object with the following attributes:
         *      logName {string} - name of the schema's log
         *      version {string} - schema version
         *
         * @return {string} Returns a link to the dashboard page
         */
        $scope.getFilteredSchemaViewLink = function (schema){
            return "#/schema/view?logName=" + schema.logName + "&version=" + schema.version;
        };

    } ]);

    /**
     * This directive extends the <input type="file"> HTML tag to also take in
     * a `filestore` attribute. The value of that attribtue will be set to the
     * contents of the file (read as text) that is selected in the input.
     *
     * Example: Using this HTML: `<input type="file" filestore="fileContent" />`
     *
     * will load the contents of the file that is selected from the input into
     * the `fileContent` variable.
     *
     * Code taken from: http://stackoverflow.com/questions/17063000/ng-model-for-input-type-file
     *
     */
    app.directive("filestore", [function () {
        return {
            scope: {
                filestore: "="
            },
            link: function (scope, element, attributes) {
                element.bind("change", function (changeEvent) {
                    var reader = new FileReader();
                    reader.onload = function (loadEvent) {
                        scope.$apply(function () {
                            scope.filestore = loadEvent.target.result;
                        });
                    };
                    reader.readAsText(changeEvent.target.files[0]);
                });
            }
        };
    }]);

} )();
