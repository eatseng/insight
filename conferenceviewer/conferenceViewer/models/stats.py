from __future__ import absolute_import
from __future__ import division

import time
from datetime import datetime, timedelta
from dateutil import tz

from clay import config
from cassandra.cluster import Cluster
from conferenceViewer.lib.redshift import Redshift

run_log = config.get_logger('conferenceViewer.run')


class Stats(object):

    QUERY_REAL_TIME_TWEETS = """SELECT * FROM insight.conference_details WHERE yymmddhh = '%s' AND timestamp > '%s'"""

    QUERY_TWEETS = """SELECT * FROM insight.conference_batch_details WHERE yymmddhh = '%s' LIMIT 10000"""

    QUERY_TEST = """SELECT * FROM insight.test1 WHERE id = 1"""

    def get_map(self):

        start_time = time.time()

        # Cassandra initialization
        cluster = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2"), config.get("cassandra.host3")])
        session = cluster.connect('insight')
        session.default_timeout = None
        session.default_fetch_size = 3000

        timestamp = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        print(self.QUERY_TWEETS % timestamp)

        tweeted_words = session.execute(self.QUERY_TWEETS % timestamp)
        # tweeted_words = session.execute(self.QUERY_TWEETS % 15020412)

        word_data = {}

        if tweeted_words == []:

            timestamp = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + (datetime.utcnow() - timedelta(hours=1)).strftime('%H')
            tweeted_words = session.execute(self.QUERY_TWEETS % timestamp)

        for (yymmddhh, location, string, data, timestamp) in tweeted_words:

            if location not in word_data:
                word_data[location] = {}

            for word, count in data.iteritems():

                word = word.encode("utf-8")

                if word not in word_data[location]:
                    word_data[location][word] = count
                else:
                    word_data[location][word] += count

        tweet_data = {}

        for location, data in word_data.iteritems():

            row = {}

            if "OVERALL_CNT" in data:
                row["tweets"] = int(data["OVERALL_CNT"])
                data.pop("OVERALL_CNT")

            loc = location.split(",")
            lat = float(loc[0])
            lng = float(loc[1])

            row["yymmddhh"] = int(timestamp)
            row["latitude"] = float(lat)
            row["longitude"] = float(lng)
            row["data"] = []

            for word, count in data.iteritems():
                if len(row["data"]) > 0 and len(word) >= len(row["data"][0]["word"]) and count >= row["data"][0]["count"]:
                    row["data"].insert(0, {"word": word, "count": count})
                else:
                    row["data"].append({"word": word, "count": count})

            tweet_data[location] = row

        # for (yymmddhh, timestamp, data, lat, lng) in tweeted_words:

        #     # Number of tweets and words at this location
        #     tweet_count = 0
        #     word_data = []

        #     # Get words from cassandra column
        #     word_count_pairs = [pair for pair in data.split(":")]

        #     for pair in word_count_pairs:

        #         data = pair.split(",")
        #         word = data[0].encode("utf-8")
        #         count = int(data[1].encode("utf-8"))

        #         if word == "OVERALL_CNT":
        #             tweet_count = count
        #             continue

        #         word_data.append({"word": word, "count": count})

        #     location = str(lat) + ", " + str(lng)

        #     # Append data if not already in record else append
        #     if location not in tweet_data:

            #     row = {}
            #     row["yymmddhh"] = int(yymmddhh)
            #     row["latitude"] = float(lat)
            #     row["longitude"] = float(lng)
            #     row["data"] = word_data
            #     row["tweets"] = tweet_count
            #     # row["word_pair"] = word_count_pairs

            #     tweet_data[location] = row

            # else:

            #     tweet_data[location]["tweets"] += tweet_count

        # # Delete misc data
        if "0.00,0.00" in tweet_data:
            tweet_data.pop("0.00,0.00")

        print("Realtime BATCH API exec time for Instance is " + str(time.time() - start_time))

        return tweet_data
        # return {}
        # return {"24.56,-81.80": {"data": [{"count": 1, "word": "ciabata"}, {"count": 1, "word": "bread"}], "latitude": 24.56, "longitude": -81.8, "tweets": 1, "yymmddhh": 1423051659018}}

    def get_updates(self, timestamp):

        start_time = time.time()

        d_time = datetime.fromtimestamp(long(timestamp.encode("utf-8")) / 1000)

        timestamp = convert_time_to_utc(d_time)

        # Cassandra initialization
        cluster = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2")])
        session = cluster.connect('insight')

        t_yymmddhh = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        print(self.QUERY_REAL_TIME_TWEETS % (t_yymmddhh, timestamp))

        tweeted_words = session.execute(self.QUERY_REAL_TIME_TWEETS % (t_yymmddhh, timestamp))
        # tweeted_words = session.execute(self.QUERY_REAL_TIME_TWEETS % "15020416")

        tweet_data = {}

        for (yymmddhh, timestamp, lat, lng, data) in tweeted_words:
            print("data")

            lat = round(float(lat), 2)
            lng = round(float(lng), 2)

            location = str(lat) + "," + str(lng)

            # Append data if not already in record else append
            if location not in tweet_data:
                tweet_data[location] = {"words": [], "tweets": 0}

            # Get words from cassandra column
            word_count_pairs = [pair for pair in data.split(":")]

            for pair in word_count_pairs:

                data = pair.split(",")
                word = data[0].encode("utf-8")
                count = int(data[1].encode("utf-8"))

                if word == "OVERALL_CNT":
                    tweet_data[location]["tweets"] += count
                    continue

                tweet_data[location]["words"].append({"word": word, "count": count})

        # Delete misc data
        if "0.0,0.0" in tweet_data:
            tweet_data.pop("0.0,0.0")
        print(tweet_data)
        print("Realtime STREAMING API exec time for Instance is " + str(time.time() - start_time))

        return tweet_data
        # return {"25.81,-80.24": {"tweets": 1, "words": [{"count": 1, "word": "shesdanger"}, {"count": 2, "word": "lol"}, {"count": 1, "word": "n"}]},
        #         "24.56,-81.80": {"tweets": 1, "words": [{"count": 1, "word": "ciabata"}, {"count": 1, "word": "putting"}]},
        #         "40,-74.00": {"tweets": 1, "words": [{"count": 5, "word": "super"}, {"count": 10, "word": "awesome"}]}
        #         }

    def test(self):

        start_time = time.time()

        # Cassandra initialization
        cluster = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2")])
        session = cluster.connect('insight')

        timestamp = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        tweeted_words = session.execute(self.QUERY_TWEETS % timestamp)

        tweeted_words = session.execute(self.QUERY_TEST)

        cnt = 0
        for (id, counter) in tweeted_words:
            print(counter)

        print(cnt)

        print("Realtime API exec time for Instance is " + str(time.time() - start_time))

        return "super"


def convert_time_to_utc(local_time):
    from_zone = tz.tzlocal()
    to_zone = tz.gettz('UTC')
    local = local_time
    local = datetime.strptime(local_time.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    local = local.replace(tzinfo=from_zone)
    return local.astimezone(to_zone)
