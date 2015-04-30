angular.module('ConferenceViewerDirectives', [])
  .directive('navbar', [
    () ->
      restrict: 'E'
      scope:
        page: "@"
      templateUrl: '../static/conferenceViewer/views/directives/navbar.html'
      link: (scope) ->  
  ])
  .directive('highlighter', ['$timeout',
    ($timeout) ->
      restrict: 'A',
      link: (scope, element, attrs) ->

        scope.$watch(attrs.highlighter,
          (nv, ov) ->
            if (nv != ov)
              element.addClass('highlight')

              $timeout(() ->
                element.removeClass('highlight')
              , 1000)
        )
  ])
