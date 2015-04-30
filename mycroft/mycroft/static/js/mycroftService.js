(function() {
    var app = angular.module('mycroftService', []);

    "use strict";
    /**
     * We defined common services shared by other ng-app(s).
     */
    app.factory('commonService', function() {
        var res = {};

        /**
         * Given new column, if it is the same as current sorting column, we toggle the sorting
         * ordering. Otherwise, we set new column as current sorting column.
         *
         * @param {object} curSort - contains sorting column(s) and order
         *      column {array}: current sorting column(s)
         *      descending {boolean}: current sorting order. True is descending; false is ascending.
         * @param {array} newSortCol - new sorting column(s)
         */
        res.sortColumn = function(curSort, newSortCol) {
            if (curSort.column.equals(newSortCol)) {
                curSort.descending = !curSort.descending;
            } else {
                curSort.column = newSortCol;
                curSort.descending = false;
            }
        };
        return res;
    });
} )();