angular.module('ConferenceViewerController', ['ConferenceViewerServices'])
  .controller('CListCtlr', ['$scope', '$filter', '$interval', 'CVService', 'SearchTableService', 'FindByService',
    ($scope, $filter, $interval, CVService, SearchTableService, FindByService) ->
      $scope.search = {}
      $scope.sort = {       
        sortingOrder : '',
        reverse : true
      }
      $scope.displayPage = 5
      $scope.filteredItems = []
      $scope.itemsPerPage = 12
      $scope.pagedItems = []
      $scope.currentPage = 0

      searchFields = ["event_name", "event_type", "start_time"]
      searchNumericFields = ["tweets"]

      angular.element("navbar").hide()
      angular.element(".page-scroller").hide()

      $scope.enter = () ->
        angular.element(".landing").css("width", "0%")
        setTimeout(() ->
          angular.element(".landing").remove()
          angular.element("navbar").show()
          angular.element(".page-scroller").show()
          angular.element(".screen").hide()
          angular.element(".title").hide()
        , 1500)

      $scope.filterList = () ->
        $scope.displayPage = 5
        $scope.filteredTextItems = $filter('filter')($scope.conference_list, SearchTableService.customFilter($scope.search, searchFields))
        $scope.filteredItems = $filter('filter')($scope.filteredTextItems, SearchTableService.numericFilter($scope.search, searchNumericFields))
        if $scope.sort.sortingOrder != ''
          $scope.filteredItems = $filter('orderBy')($scope.filteredItems, $scope.sort.sortingOrder, $scope.sort.reverse)
        $scope.currentPage = 0
        $scope.groupToPages()
        calculateStat()

      $scope.groupToPages = () ->
        $scope.pagedItems = []
        
        for i in [0...$scope.filteredItems.length]  
          if i % $scope.itemsPerPage == 0
              $scope.pagedItems[Math.floor(i / $scope.itemsPerPage)] = [ $scope.filteredItems[i] ]
          else
              $scope.pagedItems[Math.floor(i / $scope.itemsPerPage)].push($scope.filteredItems[i])
        if $scope.pagedItems.length < $scope.displayPage
          $scope.displayPage = $scope.pagedItems.length
      
      $scope.range = (size,start, end) ->
          n = size - start
          ret = []

          if size < end
            end = size;
            start = size-$scope.displayPage
          
          if n < 5 or start == 0
            for i in [start...end]
              ret.push(i)
          else if start == 1
            for i in [start-1...end-1]
                ret.push(i)
          else
            for i in [start-2...end-2]
              ret.push(i)
          ret
      
      $scope.prevPage = () ->
          if ($scope.currentPage > 0)
            $scope.currentPage--
      
      $scope.nextPage = () ->
          if ($scope.currentPage < $scope.pagedItems.length - 1)
            $scope.currentPage++
      
      $scope.setPage = (event) ->
        $scope.currentPage = this.n

      calculateStat = () ->
        now = new Date
        $scope.today = 0
        $scope.tomorrow = 0
        $scope.last7days = 0
        $scope.liveConf = 0
        $scope.liveTweets = 0
        $scope.livePart = 0
        angular.forEach($scope.filteredItems,
          (element) ->
            # console.log(new Date(element["start_time"]))
            start_time = new Date(element["start_time"])
            end_time = new Date(element["end_time"])

            if now.getDate() > end_time.getDate()
              $scope.last7days += 1
            else if now.getDate() < start_time.getDate()
              $scope.tomorrow += 1
            else if (now.getDate() >= start_time.getDate() and now.getDate() <= end_time.getDate())
              $scope.today += 1

            if now >= new Date(element["start_time"]) and now <= new Date(element["end_time"])
              $scope.liveConf += 1
              $scope.liveTweets += element["tweets"]
              $scope.livePart += element["capacity"]
        )

      $scope.goToConf = (confId) ->
        FindByService.submitConfId(confId)

      CVService.getConferences({
        "list": new Date().getTime()
        },
        (callback) ->
          $scope.conference_list = callback.conferences["data"]
          $scope.last7days = callback.conferences["last7days"]
          $scope.tomorrow = callback.conferences["tomorrow"]
          $scope.today = callback.conferences["today"]
          $scope.live_conf = callback.conferences["live_conf"]
          $scope.capacity = callback.conferences["capacity"]
          $scope.filterList()
      )        
  ])
  .controller('CDetailCtlr', ['$scope', '$routeParams', '$filter', 'CVService', 'SearchTableService',
    ($scope, $routeParams, $filter, CVService, SearchTableService) ->
      $scope.search = {}
      $scope.sort = {       
        sortingOrder : '',
        reverse : true
      }
      $scope.displayPage = 5
      $scope.filteredItems = []
      $scope.itemsPerPage = 21
      $scope.pagedItems = []
      $scope.currentPage = 0

      searchFields = ["word"]
      searchNumericFields = []

      $scope.filterList = () ->
        $scope.displayPage = 5
        $scope.filteredTextItems = $filter('filter')($scope.photos, SearchTableService.customFilter($scope.search, searchFields))
        $scope.filteredItems = $filter('filter')($scope.filteredTextItems, SearchTableService.numericFilter($scope.search, searchNumericFields))
        if $scope.sort.sortingOrder != ''
          $scope.filteredItems = $filter('orderBy')($scope.filteredItems, $scope.sort.sortingOrder, $scope.sort.reverse)
        $scope.currentPage = 0
        $scope.groupToPages()

      $scope.groupToPages = () ->
        $scope.pagedItems = []
        
        for i in [0...$scope.filteredItems.length]  
          if i % $scope.itemsPerPage == 0
              $scope.pagedItems[Math.floor(i / $scope.itemsPerPage)] = [ $scope.filteredItems[i] ]
          else
              $scope.pagedItems[Math.floor(i / $scope.itemsPerPage)].push($scope.filteredItems[i])
        if $scope.pagedItems.length < $scope.displayPage
          $scope.displayPage = $scope.pagedItems.length
      
      $scope.range = (size,start, end) ->
          n = size - start
          ret = []

          if size < end
            end = size;
            start = size-$scope.displayPage
          
          if n < 5 or start == 0
            for i in [start...end]
              ret.push(i)
          else if start == 1
            for i in [start-1...end-1]
                ret.push(i)
          else
            for i in [start-2...end-2]
              ret.push(i)
          ret
      
      $scope.prevPage = () ->
          if ($scope.currentPage > 0)
            $scope.currentPage--
      
      $scope.nextPage = () ->
          if ($scope.currentPage < $scope.pagedItems.length - 1)
            $scope.currentPage++
      
      $scope.setPage = (event) ->
        $scope.currentPage = this.n

      CVService.getConferences({
        "conf_id": $routeParams.conf_id
        },
        (callback) ->
          $scope.conf = callback.conferences["data"][0]
          $scope.photos = callback.conferences["photo_urls"]
          $scope.filterList()
      )      

  ])
  .controller('CVCtlr', ['$scope', '$interval', 'CVService',
    ($scope, $interval, CVService) ->

      # $scope.top_10_words = [{
      #     "word": "like"
      #     "count": 4537
      #     "pic_url": ["http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10914366_1616132181949968_61775286_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895338_593388997464373_1642992258_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg"]
      #   }, {
      #     "word": "get"
      #     "count": 3234
      #     "pic_url": ["http://scontent-b.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10919751_776344272448171_504973286_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895338_593388997464373_1642992258_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg"]
      #   },{
      #     "word": "all"
      #     "count": 2337
      #     "pic_url": ["http://scontent-b.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10919751_776344272448171_504973286_n.jpg", "http://scontent-b.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10919751_776344272448171_504973286_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg"]
      #   },{
      #     "word": "like"
      #     "count": 2137
      #     "pic_url": ["http://scontent-b.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10919751_776344272448171_504973286_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895338_593388997464373_1642992258_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg"]
      #   },{
      #     "word": "like"
      #     "count": 1237
      #     "pic_url": ["http://scontent-b.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10919751_776344272448171_504973286_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895338_593388997464373_1642992258_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg"]
      #   }
      # ]

      $scope.selected = {
          "word": "like"
          "count": 4537
          "pic_url": ["http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10914366_1616132181949968_61775286_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895338_593388997464373_1642992258_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg", "http://scontent-a.cdninstagram.com/hphotos-xaf1/t51.2885-15/e15/10895325_338989432959178_1669603558_n.jpg"]
      }

      $scope.mouseover = (data) ->
        $scope.selected = data
        loadFrame()

      loadFrame = () ->
        for i in [0, 1, 2, 3]
          angular.element(".frame" + i).css("background-image": "url(" + $scope.selected["pic_url"][i] + ")")

      changeSet = () ->
        $scope.selected["pic_url"].push($scope.selected["pic_url"].shift())
        $scope.selected["pic_url"].push($scope.selected["pic_url"].shift())
        randomNum = Math.floor((Math.random() * 4))
        index = (randomNum + 1) * 3
        tempurl = $scope.selected["pic_url"][index] 
        $scope.selected["pic_url"][index] = $scope.selected["pic_url"][randomNum]
        $scope.selected["pic_url"][randomNum] = tempurl
        loadFrame()

      getUpdate = () ->
        CVService.getConferences({},
          (callback) -> 
            $scope.top_10_words = callback.conferences
            $scope.selected = $scope.top_10_words[0]
            loadFrame()
            updateFrame = $interval(getUpdate, 10000)
        )

      getUpdate()

      $scope.$on('$destroy', 
        () -> 
          $interval.cancel(updateFrame)
      )

  ])
  .controller('TestCtlr', ['$scope', 'CVService'
    ($scope, CVService) ->
      $scope.titems = []
      console.log("HELLO")

      # $scope.$watch('titems', () ->
      #  alert('hey, titems has changed!')
      #  console.log($scope.titems)
      # );
      
      CVService.getConferences({},
        (callback) -> 
          $scope.titems = callback.conferences
          console.log("get stuff")
          console.log(callback)
      )

      setTimeout(
        () ->
          $scope.titems.push(13)
          $scope.anotheritems = [1, 2, 3]
          console.log("tmieout HELLO")
          console.log($scope.anotheritems)
      , 1000)
  ])