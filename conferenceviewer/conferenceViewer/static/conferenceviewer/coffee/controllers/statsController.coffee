angular.module('StatsController', ['StatsServices', 'MapServices', 'ConferenceViewerServices'])
  .controller('StatsCtlr', ['$scope', '$interval', '$rootScope', '$routeParams', '$filter', 'SService','MapService', 'CVService',
    ($scope, $interval, $rootScope, $routeParams, $filter, SService, MapService, CVService) ->
      $scope.stats = {"raw": {}, "data":[]}
      $scope.lastLoad = Date.now()
      $scope.tweetStats = {"sf_cnt": 0, "ny_cnt": 0}
      $scope.wordList = []
      $scope.settings = {"batch": true, "rt": true, "heat": false, "eb": false}

      start = Date.now()

      $scope.close = () ->
        angular.element(".datamap-click").remove()
        null

      setBackDrop = () ->
        setTimeout(
          () ->
            angular.element(".backdrop").css("background-color", "black")
            angular.element(".conf-navbar > div").css("background-color", "black")
            angular.element(".conf-navbar > div").css("color", "white")
            angular.element(".conf-navbar > div").css("border", "1px solid white")
            angular.element("#home").css("color", "white")
        , 500)

      setD3DivHeight = () ->
        width = angular.element(window).width()
        height = width / 3

        angular.element("#d3_container").css("height", height)
        angular.element("#d3_stats_container").css("height", height)

      calculateStats = () ->
        data = SService.calcStats($scope.stats["data"], $scope.tweetStats)
        $scope.wordList = data["wordList"]
        $scope.stats["data"] = data["stats"] 
        if $scope.settings["heat"] is true
          angular.element(".legend").remove()
          angular.element(".tile").remove()
          MapService.addTrafficTiles($scope.stats["data"], $scope.tweetStats["average"], $scope.tweetStats["stdev"])
        else
          template = Handlebars.compile(document.getElementById("tweet-info-template").innerHTML)
          MapService.updateOnlineMap($scope.stats["data"], template)

      $scope.batchMode = () ->
        if $scope.settings["batch"] is true
          $scope.stats = {"raw": {}}
          $scope.tweetStats = {"sf_cnt": 0, "ny_cnt": 0}
          $scope.settings["batch"] = false
          angular.element(".button#batch").removeClass("active")
          calculateStats()
        else
          angular.element(".button#batch").addClass("active")
          $scope.settings["batch"] = true
          loadBatch()

      $scope.rtMode = () ->
        if $scope.settings["rt"] is true
          $interval.cancel($scope.realTime)
          angular.element(".button#rt").removeClass("active")
          $scope.settings["rt"] = false
        else
          angular.element(".button#rt").addClass("active")
          loadRealUpdates()
          $scope.realTime = $interval(loadRealUpdates, 10000)
          $scope.settings["rt"] = true

      $scope.heatMode = () ->
        if $scope.settings["heat"] is false
          # Turn off EB Mode
          $scope.settings["eb"] = true
          $scope.ebMode()
          # Turn On Heat Map
          angular.element(".bubbles").hide()
          angular.element(".legend").remove()
          angular.element(".tile").remove()
          angular.element(".button#heat").addClass("active")
          $scope.settings["heat"] = true
          MapService.addTrafficTiles($scope.stats["data"], $scope.tweetStats["average"], $scope.tweetStats["stdev"])
        else
          angular.element(".bubbles").show()
          template = Handlebars.compile(document.getElementById("tweet-info-template").innerHTML)
          MapService.updateOnlineMap($scope.stats["data"], template)
          angular.element(".legend").hide()
          angular.element(".tile").hide()
          angular.element(".button#heat").removeClass("active")
          $scope.settings["heat"] = false

      $scope.ebMode = () ->
        if $scope.settings["eb"] is false
          # Turn off Heat Map
          $scope.settings["heat"] = true
          $scope.heatMode()
          # Turn on EB Mode
          loadEBData()
          angular.element(".button#eb").addClass("active")
          $scope.settings["eb"] = true
          # Turn off Real Time
          $interval.cancel($scope.realTime)
        else
          # Turn off Heat Map
          $scope.settings["heat"] = true
          $scope.heatMode()

          angular.element(".button#eb").removeClass("active")
          $scope.settings["eb"] = false
          # Turn on Real Time
          $scope.realTime = $interval(loadRealUpdates, 10000)

      loadEBData = () ->
        CVService.getConferences({
          "list": new Date().getTime()
          },
          (callback) ->
            $scope.conference_list = callback.conferences["data"]
            $scope.conference_list = MapService.transformEBData($scope.conference_list)
            console.log($scope.conference_list)
            template = Handlebars.compile(document.getElementById("eb-info-template").innerHTML)
            MapService.updateOnlineMap($scope.conference_list, template)
        )        

      loadRealUpdates = () ->
        console.log("queue real time")
        SService.getRealTime({
            "real_time": true
            "time_stamp": $scope.lastLoad
          },
          (callback) ->
            $scope.lastLoad = Date.now()
            SService.mergeData($scope.stats, callback)

            # Load data to map
            calculateStats()
            template = Handlebars.compile(document.getElementById("tweet-info-template").innerHTML)
            MapService.updateOnlineMap($scope.stats["data"], template)

            console.log("Run time: " + (Date.now() - start))
            $scope.tweetData = []
            $scope.tweetData.push({xAxis: "SF", "Tweets": $scope.tweetStats["sf_cnt"]})
            $scope.tweetData.push({xAxis: "NYC", "Tweets": $scope.tweetStats["ny_cnt"]})
            $rootScope.$broadcast('tweet')
            start = Date.now()
        )    
      loadBatch = () ->
        SService.loadRawData({
          "conf_id": $routeParams.conf_id
          },
          (callback) ->
            $scope.stats = callback

            # Load data to map
            calculateStats()
            template = Handlebars.compile(document.getElementById("tweet-info-template").innerHTML)
            MapService.updateOnlineMap($scope.stats["data"], template)

            # console.log($scope.stats["raw"])
            
        )    

      loadBatch()
      setBackDrop()
      setD3DivHeight()
      MapService.initializeDataMap($scope, "d3_stats_container")
      # loadRealUpdates()

      # $scope.realTime = $interval(loadRealUpdates, 5000)

      $scope.$on('$destroy', 
        () -> 
          angular.element(".backdrop").css("background-color", "white")
          angular.element(".conf-navbar > div").css("background-color", "white")
          angular.element(".conf-navbar > div").css("color", "black")
          angular.element(".conf-navbar > div").css("border", "1px solid black")
          angular.element("#home").css("color", "black")
          $interval.cancel($scope.realTime)
      )

  ])