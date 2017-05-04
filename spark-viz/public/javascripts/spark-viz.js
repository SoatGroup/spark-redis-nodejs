/**
 * Created by brobrien on 10/26/16.
 */
'use strict';

var sparkVizModule = angular.module('sparkviz', ['btford.socket-io', 'chart.js']);

//This block is for plugging socket io events into the angularjs rendering life cyle
sparkVizModule.factory('socket', function ($rootScope) {
    var socket = io('/');
    return {
        on: function (eventName, callback) {
            socket.on(eventName, function () {
                var args = arguments;
                $rootScope.$apply(function () {
                    callback.apply(socket, args);
                });
            });
        },
        emit: function (eventName, data, callback) {
            socket.emit(eventName, data, function () {
                var args = arguments;
                $rootScope.$apply(function () {
                    if (callback) {
                        callback.apply(socket, args);
                    }
                });
            })
        }
    };
});

function drawCharts() {
    var chartAnchor = document.getElementById('dataflow');
    if (chartAnchor) {
        var chart = new google.visualization.Sankey(chartAnchor);
        renderChart(chart);
        setInterval(function () {
            renderChart(chart);
        }, 3000);
    }
}

var streamTransitions = [];

function renderChart(chart) {
    var data = new google.visualization.DataTable();
    data.addColumn('string', 'From');
    data.addColumn('string', 'To');
    data.addColumn('number', 'Weight');
    data.addRows(streamTransitions);
    chart.draw(data, chartOptions);
}

google.charts.load('current', {
    'packages': ['sankey']
});

google.charts.setOnLoadCallback(drawCharts);

var colors = ['#a6cee3', '#b2df8a', '#fb9a99', '#fdbf6f',
    '#cab2d6', '#ffff99', '#1f78b4', '#33a02c'];

var chartOptions = {
    width: '100%',
    sankey: {
        node: {
            nodePadding: 80,
            width: 3,
            colors: colors
        },
        link: {
            colorMode: 'gradient',
            colors: colors
        }
    }
};

sparkVizModule.controller('SparkVizController', ['$scope', '$http', 'socket', function ($scope, $http, socket) {

    $scope.totalWords = 0;
    $scope.totalCharacters = 0;
    $scope.avgWordLength = 0.0;

    socket.on('spark-metrics', function (msg) {
        //var data = JSON.parse(msg);
        //console.log(msg);
        $scope.data = msg;
    });

    socket.on('stream-transitions', function (msg) {
        var transitions = JSON.parse(msg);

        streamTransitions = [];

        for (var key in transitions) {
            var t = transitions[key];
            //console.log(t)
            streamTransitions.push([t.from, t.to, t.count === 0 ? 1 : t.count]); //count + 1 to avoid chart rendering problems
        }
        //console.log(transitions);
    });

    socket.on('detail-metrics', function (msg) {
        var detailMetrics = JSON.parse(msg);
        //console.log(detailMetrics);
        $scope.detailMetrics = detailMetrics;
    });

    $scope.words = [];
    socket.on('raw-messages', function (msg) {
        $scope.words.unshift(msg);

        if ($scope.words.length > 15)
            $scope.words.pop();

    });

    socket.on('summary-metrics', function (msg) {
        var summaryMetrics = JSON.parse(msg);
        //console.dir(summaryMetrics);
        $scope.avgWordLength = summaryMetrics.avgCharsPerWord.toFixed(2);
        $scope.totalWords = summaryMetrics.totalWords;
        $scope.totalCharacters = summaryMetrics.totalCharacters;
        $scope.lastRDDTime = summaryMetrics.lastRDDTime;
    });

    socket.on('time', function (msg) {
        $scope.time = msg;
    });
}]);