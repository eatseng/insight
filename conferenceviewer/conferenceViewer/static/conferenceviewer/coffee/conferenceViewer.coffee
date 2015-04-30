angular.module('conferenceViewer', 
  ['ngRoute',
   'ngResource',
   'ngAnimate',
   'ConferenceViewerController',
   'StatsController',
   'ConferenceViewerDirectives',
   'ConferenceDirectives'
  ])
  .config(['$routeProvider',
    ($routeProvider) ->
      $routeProvider.
        when('/',
          templateUrl: '../static/conferenceViewer/views/conferences/conference_list.html'
          controller: 'CListCtlr'
        ).
        when('/conference/detail/:conf_id',
          templateUrl: '../static/conferenceViewer/views/conferences/conference_detail.html'
          controller: 'CDetailCtlr'
        ).
        when('/stats',
          templateUrl: '../static/conferenceViewer/views/stats/map.html'
          controller: 'StatsCtlr'
        ).
        when('/test',
          templateUrl: '../static/conferenceViewer/views/test.html'
          controller: 'TestCtlr'
        ).
        when('/404',
          templateUrl: '../static/conferenceViewer/views/404_not_found.html'
        ).
        otherwise(
          templateUrl: '../static/conferenceViewer/views/404_not_found.html'
        )
  ])
 