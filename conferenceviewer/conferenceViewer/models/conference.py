from __future__ import absolute_import
from __future__ import division

import time
import operator
from datetime import datetime
from dateutil import tz

from clay import config
from cassandra.cluster import Cluster
from conferenceViewer.lib.redshift import Redshift

from instagram.client import InstagramAPI

run_log = config.get_logger('conferenceViewer.run')


class Conference(object):

    TOP_10_QUERY = """SELECT * FROM insight.tweet_rankings WHERE yymmdd = '%s' ORDER BY count DESC LIMIT 10;"""

    QUERY_TWEETS = """SELECT * FROM insight.conference_details WHERE yymmddhh = '%s'"""

    CONFERNECE_LIST_QUERY = """SELECT
            DISTINCT
            ebe.eb_id as eb_id,
            ebe.event_name as event_name,
            ebe.event_type as event_type,
            ebe.logo_url as logo_url,
            ebe.start_time_utc as start_time_utc,
            ebe.end_time_utc as end_time_utc,
            ebe.capacity as capacity,
            ebe.venue_name as venue_name,
            ebe.latitude as latitude,
            ebe.longitude as longitude,
            tac.tweets as tweets
        FROM eb_daily_events AS ebe
        LEFT JOIN time_acc_conferences AS tac
        ON ebe.eb_id = tac.eb_id
        WHERE ebe.end_time_utc > GETDATE() - INTERVAL '7 day'
    """

    CONFERNECE_DETAIL_QUERY = """SELECT
            ebe.event_name as event_name,
            ebe.event_type as event_type,
            ebe.logo_url as logo_url,
            ebe.start_time_utc as start_time_utc,
            ebe.end_time_utc as end_time_utc,
            ebe.latitude as latitude,
            ebe.longitude as longitude,
            tac.tweets,
            tac.word1, tac.word2, tac.word3, tac.word4, tac.word5,
            tac.word6, tac.word7, tac.word8, tac.word9, tac.word10,
            tac.count1, tac.count2, tac.count3, tac.count4, tac.count5,
            tac.count6, tac.count7, tac.count8, tac.count9, tac.count10
        FROM eb_daily_events AS ebe
        LEFT JOIN time_acc_conferences AS tac
        ON ebe.eb_id = tac.eb_id
        WHERE ebe.end_time_utc > GETDATE() - INTERVAL '7 day'
        AND ebe.eb_id = '%s'
        ORDER BY ebe.created_at DESC
    """

    GET_PHOTOS_QUERY = """SELECT
            *
        FROM keyword_photos
        WHERE word IN (%s)
        ORDER BY random()
        LIMIT 200;
    """
    COLORS = ["rgb(0, 66, 140)", "rgb(0, 100, 181)", "rgb(58, 150, 193)", "rgb(242, 167, 52)", "rgb(125, 200, 204)",
              "rgb(225, 167, 181)", "rgb(234, 184, 146)", "rgb(0, 16, 99)", "rgb(51, 51, 51)", "rgb(209, 184, 46)",
              "rgb(125, 33, 40)", "rgb(25, 50, 105)"
              ]

    def get_conference_detail(self, eb_id):

        # Get data from redshift
        db = Redshift()

        data = db.select(self.CONFERNECE_DETAIL_QUERY % eb_id)

        bag_of_words = []

        json = {"data": [], "photo_urls": []}

        for row in data:

            eb_data = {"tweeted_words": []}

            eb_data["event_name"] = row["event_name"] if row["event_name"] else "N/A"
            eb_data["event_type"] = row["event_type"] if row["event_type"] else "N/A"
            eb_data["logo_url"] = row["logo_url"] if row["logo_url"] else None
            eb_data["start_time"] = convert_time_to_local(row["start_time_utc"]).strftime('%Y-%m-%d %H:%M:%S') if row["start_time_utc"] else None
            eb_data["end_time"] = convert_time_to_local(row["end_time_utc"]).strftime('%Y-%m-%d %H:%M:%S') if row["end_time_utc"] else None
            eb_data["latitude"] = row["latitude"] if row["latitude"] else None
            eb_data["longitude"] = row["longitude"] if row["longitude"] else None
            eb_data["tweets"] = row["tweets"] if row["tweets"] else 0

            for i in xrange(10):
                if row["word" + str(i + 1)] is not None:
                    eb_data["tweeted_words"].append({"word": row["word" + str(i + 1)], "count": row["count" + str(i + 1)]})
                    # Get all the words that appeared in the conference
                    bag_of_words.append("'" + row["word" + str(i + 1)] + "'")

            json["data"].append(eb_data)

            if len(bag_of_words):

                # Get photo data from Redshift
                for row in db.select(self.GET_PHOTOS_QUERY % ','.join(bag_of_words)):

                    photo_data = {}
                    photo_data["word"] = row["word"]
                    photo_data["url"] = row["url"]
                    photo_data["colors"] = self.COLORS[1]

                    # Rotate colors
                    self.COLORS = self.COLORS[1:] + self.COLORS[:1]
                    json["photo_urls"].append(photo_data)

            break

        return json

    def get_conference_list(self, date):

        json = {"data": [], "today": 0, "tomorrow": 0, "last7days": 0, "live_conf": 0, "capacity": 0, "live_tweets": 0}

        # Get data from redshift
        db = Redshift()

        data = db.select(self.CONFERNECE_LIST_QUERY)

        for row in data:

            if row["latitude"] is None and row["longitude"] is None:
                continue

            eb_data = {}

            eb_data["eb_id"] = row["eb_id"] if row["eb_id"] else None
            eb_data["event_name"] = row["event_name"] if row["event_name"] else None
            eb_data["event_type"] = row["event_type"] if row["event_type"] else " "
            eb_data["logo_url"] = row["logo_url"] if row["logo_url"] else None
            eb_data["start_time"] = convert_time_to_local(row["start_time_utc"]).strftime('%Y-%m-%d %H:%M:%S') if row["start_time_utc"] else None
            eb_data["end_time"] = convert_time_to_local(row["end_time_utc"]).strftime('%Y-%m-%d %H:%M:%S') if row["end_time_utc"] else None
            eb_data["capacity"] = row["capacity"] if row["capacity"] else 0
            eb_data["venue_name"] = row["venue_name"] if row["venue_name"] else " "
            eb_data["latitude"] = row["latitude"] if row["latitude"] else None
            eb_data["longitude"] = row["longitude"] if row["longitude"] else None
            eb_data["tweets"] = int(row["tweets"]) if row["tweets"] else 0
            eb_data["colors"] = self.COLORS[1]

            # Rotate colors
            self.COLORS = self.COLORS[1:] + self.COLORS[:1]

            json["data"].append(eb_data)

        return json

    def get_conference_pictures(self, keyword):

        json_data = []

        # Cassandra initialization
        cluster = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2")])
        session = cluster.connect('insight')

        # Instagram initialization
        instagram_api = InstagramAPI(client_id=config.get("instagram.client_id"), client_secret=config.get("instagram.client_secret"))

        yymmdd = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d')

        rows = session.execute(self.TOP_10_QUERY % (yymmdd))

        for (yymmdd, count, word) in rows:

            img_arr = []

            popular_media = instagram_api.media_popular(count=20)

            for media in popular_media:
                img_arr.append(media.images['standard_resolution'].url)

            json_data.append({"word": word, "count": count, "pic_url": img_arr})

        return json_data

    def real_time(self, keyword):

        start_time = time.time()

        json_data = []

        # Cassandra initialization
        cluster = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2")])
        session = cluster.connect('insight')

        # Instagram initialization
        instagram_api = InstagramAPI(client_id=config.get("instagram.client_id"), client_secret=config.get("instagram.client_secret"))

        lat_l, lng_l = 36.8, -122.75
        lat_h, lng_h = 37.8, -121.75

        word_data = {}

        timestamp = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        tweeted_words = session.execute(self.QUERY_TWEETS % timestamp)

        for (yymmddhh, lat, lng, timestamp, data) in tweeted_words:

            word_count_pairs = [pair for pair in data.split(":")]

            # if lat_l < lat and lat_h > lat and lng_l < lng and lng_h > lng:
            # >>>
            for pair in word_count_pairs:

                data = pair.split(",")
                word = data[0].encode("utf-8")
                count = int(data[1].encode("utf-8"))

                if word == "OVERALL_CNT":
                    continue

                if word not in word_data:
                    word_data[word] = count
                else:
                    word_data[word] += count

        # In place sort by number of words in desc order
        sorted_by_loc = sorted(word_data.items(), key=operator.itemgetter(1))
        sorted_by_loc.reverse()

        # Get data from Instagram
        img_arr = []

        popular_media = instagram_api.media_popular(count=80)

        for media in popular_media:
            img_arr.append(media.images['standard_resolution'].url)

        for i in range(10):

            word = sorted_by_loc[i][0]
            count = sorted_by_loc[i][1]

            json_data.append({"word": word, "count": count, "pic_url": img_arr[(i * 4):((i + 1) * 4)]})

        print("Realtime API exec time for Instance is " + str(time.time() - start_time))

        return json_data


def convert_time_to_local(utc_time):
    from_zone = tz.gettz('UTC')
    to_zone = tz.tzlocal()
    utc = utc_time
    utc = datetime.strptime(utc_time.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    utc = utc.replace(tzinfo=from_zone)
    return utc.astimezone(to_zone)
