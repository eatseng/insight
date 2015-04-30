angular.module('StatsServices', ['ngResource', 'StatsAPIRoutes'])
  .service('SService', ['$resource', 'statsAPI', 'errorMsg',
    ($resource, statsAPI, errorMsg) ->
      getResource = (url) ->
        SService = $resource(url, {},
          getUserInfo:
            method: 'GET'
        )

      this.loadRawData = (params, callback) ->
          SService = getResource(statsAPI)
          SService.get(params).$promise.then(
            (data) ->
              
              data_arr = []
              
              for key in Object.keys(data.stats)
                data_arr.push(data.stats[key])

              d = {"raw": data.stats, "data": data_arr}

              callback(d)
            (error) ->
              callback(errorMsg)
          )

      this.getRealTime = (params, callback) ->
          SService = getResource(statsAPI)
          SService.get(params).$promise.then(
            (data) ->
              callback(data)
            (error) ->
              callback(errorMsg)
          )

      this.mergeData = (sourceData, realData) ->
        # Go through realtime Data
        i = 0
        for location, value of realData["stats"]

          # Check if realtime location match what's stored in angular
          if location of sourceData["raw"]
            i += 1
            # There is a match then increment tweets and dump words together
            sourceData["raw"][location]["tweets"] += value["tweets"]
            
            for word_pair in value["words"]
              sourceData["raw"][location]["data"].push(word_pair)

            #reconcile copies of same word
            sourceData["raw"][location]["data"] = aggregateDups(sourceData["raw"][location]["data"])

          else
            loc = location.split(",")
            lat = parseFloat(loc[0])
            lng = parseFloat(loc[1])
            sourceData["raw"][location] = {}
            sourceData["raw"][location]["data"] = value["words"]
            sourceData["raw"][location]["tweets"] = value["tweets"]
            sourceData["raw"][location]["latitude"] = lat
            sourceData["raw"][location]["longitude"] = lng

        sourceData["data"] = []
        for key in Object.keys(sourceData["raw"])
          sourceData["data"].push(sourceData["raw"][key])

        console.log(i)
            
      aggregateDups = (arr) ->
        hash = {}
        for word_pair in arr
          if word_pair["word"] of hash
            hash[word_pair["word"]] += word_pair["count"]
          else
            hash[word_pair["word"]] = word_pair["count"]

        arr = []
        for word, count of hash
          if arr.length and word.length >= arr[0]["word"].length and count >= arr[0]["count"]
            arr.unshift({"word": word, "count": count})
          else
            arr.push({"word": word, "count": count})

        arr

      this.calcStats = (data, tweetStats) ->
        if data is undefined or data.length is 0
          return {"stats": [], "wordList": []}

        top_10 = []
        average = 0
        variance = 0
        cnt = 0
        stdev = 0

        for el in data
          average += el["tweets"]
          cnt +=1

        average /= cnt

        # Record tweet stats
        tweetStats["average"] = parseFloat(average).toFixed(2)
        tweetStats["tweets"] = Math.round(average * cnt)
        tweetStats["locations"] = cnt

        for el in data
          variance += Math.pow(el["tweets"] - average, 2)

        variance /= cnt
        stdev = Math.pow(variance, 0.5)
        tweetStats["stdev"] = parseFloat(stdev).toFixed(2)

        for el in data
          if el["tweets"] < average
              el["fillKey"] = "below_avg"
              el["color"] = "#99FFCC"
          else if el["tweets"] < (average + stdev)
              el["fillKey"] = "one_std"
              el["color"] = "#F7CA18"
          else if el["tweets"] < (average + 2 * stdev)
              el["fillKey"] = "two_std"
              el["color"] = "#E87E04"
          else
              el["fillKey"] = "three_std"
              el["color"] = "#FF0000"

          # Get tweets from NY and SF
          if el["latitude"] >= 36.8 and el["latitude"] <= 37.8 and el["longitude"] >= -122.75 and el["longitude"] <= -121.75
            tweetStats["sf_cnt"] += 1
          if el["latitude"] >= 40 and el["latitude"] <= 41 and el["longitude"] >= -74 and el["longitude"] <= -73
            tweetStats["ny_cnt"] += 1

          # Accumlate data for top 10 words
          numWords = el["data"].length
          wordList = if numWords > 0 then el["data"] else []

          for word_pair in wordList
            if top_10.length == 0
              top_10 = [word_pair]
              continue

            i = 0
            for top_word in top_10
              if top_word["count"] < word_pair["count"] and not (i is 0 and word_pair["word"] == top["word"])
                length = 10
                top_10_copy = top_10.slice(i, length)
                top_10.splice(i, length, word_pair)
                top_10 = top_10.concat(top_10_copy)
                break
              i += 1

            if top_10.length > 9
                top_10.pop()
          
          el["radius"] = 2
          el["info"] = {"wordList": wordList, "tweets": el["tweets"]}

        return {"stats": data, "wordList": top_10}

      this
  ])