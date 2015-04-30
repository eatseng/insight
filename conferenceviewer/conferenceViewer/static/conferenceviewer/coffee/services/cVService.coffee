angular.module('ConferenceViewerServices', ['ngResource', 'ConferenceViewerURL', 'ConferenceViewerAPIRoutes'])
  .service('CVService', ['$resource', 'conferencesAPI', 'errorMsg',
    ($resource, conferencesAPI, errorMsg) ->
      getResource = (url) ->
        CVService = $resource(url, {},
          getUserInfo:
            method: 'GET'
        )

      this.getConferences = (params, callback) ->
          CVService = getResource(conferencesAPI)
          CVService.get(params).$promise.then(
            (data) ->
              angular.forEach(data["conferences"]["data"], (conf) ->
                date_arr = new Date(conf["start_time"]).toDateString().split(" ").slice(0, 3)
                time_arr = new Date(conf["start_time"]).toLocaleTimeString().split(" ")
                conf["start_str1"] = date_arr[0] + ", " + date_arr[1] + " " + date_arr[2]
                conf["start_str2"] = time_arr[0].split(":").slice(0, 2).join(":") + " " + time_arr[1]
              )
              callback(data)
            (error) ->
              callback(errorMsg)
          )
      this
  ])
  .service('SearchTableService', [
    () ->
      buildRegex = (input) ->
        regex_str = ""
        if input != undefined and input != null
          angular.forEach(input.split(""), 
            (char) ->
              if char != "*"
                regex_str += "[" + char + "]"
              else
                regex_str += "[A-Za-z]"
          )
        regex = new RegExp(regex_str, "i")

      this.customFilter = (searchInput, searchFields) ->
        return (row_value) ->
          match = true
          for field in searchFields
            if searchInput.hasOwnProperty(field) and match
              regex_str = buildRegex(searchInput[field])
              match = regex_str.test(row_value[field])
          match


      this.numericFilter = (searchInput, searchFields) ->
        return (row_value) ->
          match = true
          for field in searchFields
            if searchInput.hasOwnProperty(field) and match

              if searchInput[field] == "" or searchInput[field] == "<" or searchInput[field] == ">" or searchInput[field] == null or searchInput[field] == undefined
                match = true
              else
                match = false
              
              if !match
                greaterThan = searchInput[field].split(">")
                lessThan = searchInput[field].split("<")
                greaterThanLessThan = searchInput[field].split(">>")
                lesserThanGreaterThan = searchInput[field].split("<<")

                if greaterThan.length == 2
                  match = row_value[field] > parseInt(greaterThan[1])
                else if lessThan.length == 2
                  match = row_value[field] < parseInt(lessThan[1])
                else if greaterThanLessThan.length == 2
                  match = parseInt(greaterThanLessThan[0]) > row_value[field] and row_value[field] > parseInt(greaterThanLessThan[1]) 
                else if lesserThanGreaterThan.length == 2
                  match = parseInt(lesserThanGreaterThan[1]) > row_value[field] and row_value[field] > parseInt(lesserThanGreaterThan[0]) 
                else
                  match = row_value[field] == parseInt(searchInput[field])
          return match

      this
  ])
  .service('FindByService', ['$location', 'ConfDetailURL',
    ($location, ConfDetailURL) ->
      this.submitConfId = (confId) ->
        if confId != undefined or confId == ""
          $location.path(ConfDetailURL+String(confId).replace(/[^0-9\.]+/g, ''))
        else
          angular.element("#div-notification").notify("Nothing is entered")
          angular.element("#div-notification").slideUp(3000)
        null

      this
  ])