import json
import time
import traceback

from clay import config
from kafka import KafkaClient, SimpleProducer
from TwitterAPI import TwitterAPI
from datamonkey.lib.mysql import Mysql
from datamonkey.lib.error import error_email

script_log = config.get_logger('monkey_job')


class job(object):

    KAFKA_TOPIC = "twitterstreaming"

    QUERY_BY_LOCATIONS = """SELECT
            d.latitude,
            d.longitude,
            d.start_time_utc,
            d.end_time_utc
        FROM (
            SELECT
                DISTINCT(eb_id),
                start_time_utc,
                end_time_utc,
                latitude,
                longitude
            FROM eb_daily_events
            WHERE start_time_utc <= ADDTIME(CURRENT_TIMESTAMP(), '0 00:30:00')
            AND end_time_utc >= SUBTIME(CURRENT_TIMESTAMP(), '0 00:30:00')
            ) AS d
        """

    REFRESH_INTERVAL = 30 * 60

    GEO_BOX = [-0.05, 0.05]

    MYSQL = Mysql()

    def run(self, options=None):

        kafka = KafkaClient(config.get("kafka.host1") + "," + config.get("kafka.host2"))

        producer = SimpleProducer(kafka)

        try:
            while True:

                api = TwitterAPI(consumer_key=config.get("twitter.consumer_key"),
                                 consumer_secret=config.get("twitter.consumer_secret"),
                                 access_token_key=config.get("twitter.access_token"),
                                 access_token_secret=config.get("twitter.access_token_secret")
                                 )

                # tweeter_stream = api.request('statuses/filter', {'locations': self.get_geo_str()})
                tweeter_stream = api.request('statuses/filter', {'locations': "-123.66,32.54,-113.77,39.57,-93.82,24.32,-65.08,47.84"})

                start_time = time.time()

                # Stream data
                for tweet in tweeter_stream:

                    # Break out for loop at specified time interval to query new sets of Geo Coordinates
                    if time.time() > start_time + self.REFRESH_INTERVAL:
                        break

                    # Publish tweets to Kafka
                    producer.send_messages(self.KAFKA_TOPIC, json.dumps(tweet))

        except Exception, e:
            print(e)
            kafka.close()

            script_log.error("Job %s failed: %s" % (self.TABLE_NAME, e))
            error_email(table_name=self.KAFKA_TOPIC, error=e, trace=(''.join(traceback.format_stack())))

    def get_geo_str(self):

        geo_boxes = []

        geo_data = self.MYSQL.select(self.QUERY_BY_LOCATIONS)

        for row in geo_data:

            lat = float(row[0])
            lng = float(row[1])

            # geo_str = ",".join([str(round(lat + self.GEO_BOX[0], 4)), str(round(lng + self.GEO_BOX[0], 4)),
            #                     str(round(lat + self.GEO_BOX[1], 4)), str(round(lng + self.GEO_BOX[1], 4))])

            geo_str = ",".join([str(round(lng + self.GEO_BOX[0], 4)), str(round(lat + self.GEO_BOX[0], 4)),
                                str(round(lng + self.GEO_BOX[1], 4)), str(round(lat + self.GEO_BOX[1], 4))])

            geo_boxes.append(geo_str)

        return ",".join(geo_boxes)

    def check_stream(self):
        api = TwitterAPI(consumer_key=config.get("twitter.consumer_key"),
                         consumer_secret=config.get("twitter.consumer_secret"),
                         access_token_key=config.get("twitter.access_token"),
                         access_token_secret=config.get("twitter.access_token_secret")
                         )

        while True:

            tweeter_stream = api.request('statuses/filter', {'locations': "-123.66,32.54,-113.77,39.57,-93.82,24.32,-65.08,47.84"})

            # tweeter_stream = api.request('statuses/filter', {'locations': self.get_geo_str()})
            # print(self.get_geo_str())

            start_time = time.time()

            # print("len")
            # print((tweeter_stream.text))
            # print((tweeter_stream.stream))

            # Stream data
            for tweet in tweeter_stream:

                # Break out for loop at specified time interval to query new sets of Geo Coordinates
                if time.time() > start_time + self.REFRESH_INTERVAL:
                    print("breaktime")
                    break

                # Publish tweets to Kafka
                print(tweet)

    def test_ts(self):
        kafka = KafkaClient(config.get("kafka.url"))

        producer = SimpleProducer(kafka)

        api = TwitterAPI(consumer_key=config.get("twitter.consumer_key"),
                         consumer_secret=config.get("twitter.consumer_secret"),
                         access_token_key=config.get("twitter.access_token"),
                         access_token_secret=config.get("twitter.access_token_secret")
                         )

        tweeter_stream = api.request('statuses/filter', {'locations': '-122.75,36.8,-121.75,37.8,-74,40,-73,41'})

        # Stream data
        for tweet in tweeter_stream:
            producer.send_messages("test", json.dumps(tweet))
            break


if __name__ == "__main__":
    job = job()
    # job.check_stream()
    job.run()
    # CLAY_CONFIG=./config/local.yaml python -m jobs.kafka.twitter_processes.tweeter_stream_producer
