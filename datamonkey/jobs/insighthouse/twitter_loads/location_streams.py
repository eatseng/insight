import re
import json
import time
import traceback
import calendar
from datetime import datetime

from clay import config
from kafka import KafkaClient, SimpleConsumer
from datamonkey.lib.error import error_email
from boto.s3.connection import S3Connection
from datamonkey.lib.redshift import Redshift
from boto.s3.key import Key
from cStringIO import StringIO

script_log = config.get_logger('monkey_job')


class job(object):

    KAFKA_TOPIC = "twitterstreaming"

    GROUP_NAME = "twitter_s3"

    TABLE_NAME = KAFKA_TOPIC + "_" + "kafka_offsets"

    PARTITION = 1

    BATCH_SIZE = 50000

    CREATE_TRACKING_TABLE = """CREATE TABLE %s (
            group_name VARCHAR(128),
            k_partition INT,
            k_offset INT8,
            created_at TIMESTAMP,
            PRIMARY KEY (group_name, k_offset)
        )
        DISTSTYLE EVEN
        SORTKEY(group_name);
        """ % TABLE_NAME

    GET_OFFSET_QUERY = """SELECT
                MAX(k_offset)
            FROM %s
            WHERE group_name = '%s'
        """ % (TABLE_NAME, GROUP_NAME)

    UPDATE_OFFSET_QUERY = """INSERT INTO twitterstreaming_kafka_offsets
            (group_name, k_partition, k_offset, created_at) VALUES ('%s',%d,%d,getdate())
        """

    S3_KEY = KAFKA_TOPIC

    REDSHIFT = Redshift()

    def run(self, options=None):

        # try:

        # Create table if it doesn't exist in the database
        if self.REDSHIFT.if_table_exists(self.TABLE_NAME) is False:
            self.REDSHIFT.execute(self.CREATE_TRACKING_TABLE)

        kafka = KafkaClient(config.get("kafka.host1") + "," + config.get("kafka.host2"))

        consumer = SimpleConsumer(kafka, self.GROUP_NAME, self.KAFKA_TOPIC, fetch_size_bytes=3000000,
                                  buffer_size=2000000000, max_buffer_size=2000000000)

        while True:

            # Prepare data for insert and copy to S3
            data_str = StringIO()
            csv_str = StringIO()
            count = 0

            # Get Offset from previous read
            s3_last_offset = self.get_s3_offset()

            (last_offset) = self.REDSHIFT.select(self.GET_OFFSET_QUERY)[0][0]
            last_offset = last_offset if last_offset else 0

            # Resolve difference in offset (s3 offset does not carry over from day to day)
            if s3_last_offset > last_offset:
                last_offset = s3_last_offset
                self.REDSHIFT.execute(self.UPDATE_OFFSET_QUERY % (self.GROUP_NAME, self.PARTITION, last_offset))

            print(last_offset)

            # Read from Offset
            consumer.seek(last_offset, 0)

            for message in consumer.get_messages(count=self.BATCH_SIZE, block=False, timeout=5):

                # Write tweets to StringIO
                self.write_to_data_str(message, data_str, csv_str)

                count += 1
                last_offset += 1

            # Store batch tweets to S3
            self.write_to_s3(data_str, csv_str, last_offset)

            # Track Kafka Offset
            self.REDSHIFT.execute(self.UPDATE_OFFSET_QUERY % (self.GROUP_NAME, self.PARTITION, last_offset))

            if count != self.BATCH_SIZE:
                break

        # except Exception, e:
        #     print(e)
        #     kafka.close()

        #     script_log.error("Job %s failed: %s" % (self.GROUP_NAME, e))
            # error_email(table_name=self.GROUP_NAME, error=e, trace=(''.join(traceback.format_stack())))

    def get_s3_offset(self):

        # Get s3 bucket
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        s3_bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)

        mmddyy = datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%y')

        s3_key = self.KAFKA_TOPIC + "/" + mmddyy + "/"

        offset = [int(key.name.split("_")[2]) for key in s3_bucket.list(prefix=s3_key) if ".txt" in key.name]

        offset = offset if len(offset) else [0]

        s3_connection.close()

        return max(offset)

    def write_to_data_str(self, message, data_str, csv_str):

        json_data = json.loads(message.message.value)

        if "id" not in json_data:
            return

        tweet_data = {}

        flat_attributes = ["id", "id_str", "text", "retweeted", "retweet_count", "favorited", "favorite_count",
                           "lang", "source", "timestamp_ms", "in_reply_to_status_id", "in_reply_to_screen_name",
                           "in_reply_to_user_id", "in_reply_to_user_id_str", "possibly_sensitive", "created_at", "in_reply_to_status_id_str"
                           ]

        for attribute in flat_attributes:
            if attribute == "created_at":
                tweet_data[attribute] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(json_data[unicode("timestamp_ms")]) / 1000)) if json_data[unicode("timestamp_ms")] else None
            else:
                tweet_data[attribute] = json_data[unicode(attribute)] if json_data[unicode(attribute)] else None

        entities_attributes = ["user_mentions", "hashtags"]

        tweet_data["user_mentions"] = " ".join([self.strip_str(f[unicode("screen_name")]) for f in json_data[unicode("entities")][unicode("user_mentions")]]) if json_data[unicode("entities")] and json_data[unicode("entities")][unicode("user_mentions")] else None
        tweet_data["hashtags"] = " ".join([self.strip_str(f[unicode("text")]) for f in json_data[unicode("entities")][unicode("hashtags")]]) if json_data[unicode("entities")] and json_data[unicode("entities")][unicode("hashtags")] else None

        geo_attributes = ["lat", "lng", "place", "country"]

        tweet_data["lat"] = json_data[unicode("geo")][unicode("coordinates")][0] if json_data[unicode("geo")] and json_data[unicode("geo")][unicode("coordinates")] else None
        tweet_data["lng"] = json_data[unicode("geo")][unicode("coordinates")][1] if json_data[unicode("geo")] and json_data[unicode("geo")][unicode("coordinates")] else None
        tweet_data["place"] = self.strip_str(json_data[unicode("place")][unicode("full_name")]) if json_data[unicode("place")] and json_data[unicode("place")][unicode("full_name")] else None
        tweet_data["country"] = self.strip_str(json_data[unicode("place")][unicode("country_code")]) if json_data[unicode("place")] and json_data[unicode("place")][unicode("country_code")] else None

        user_attributes = ["user-id", "user-id_str", "name", "screen_name", "verified", "description",
                           "statuses_count", "followers_count", "friends_count", "listed_count",
                           "location", "user-lang", "user-created_at"
                           ]

        for attribute in user_attributes:
            tweet_data[attribute] = self.strip_str(json_data[unicode("user")][unicode(attribute)]) if "user-" not in attribute and json_data[unicode("user")] and json_data[unicode("user")][unicode(attribute)] else None

        tweet_data["user-id"] = json_data[unicode("user")][unicode("id")] if json_data[unicode("user")] and json_data[unicode("user")][unicode("id")] else None
        tweet_data["user-id_str"] = self.strip_str(json_data[unicode("user")][unicode("id_str")]) if json_data[unicode("user")] and json_data[unicode("user")][unicode("id_str")] else None
        tweet_data["user-lang"] = json_data[unicode("user")][unicode("lang")] if json_data[unicode("user")] and json_data[unicode("user")][unicode("lang")] else None
        tweet_data["user-created_at"] = json_data[unicode("user")][unicode("created_at")] if json_data[unicode("user")] and json_data[unicode("user")][unicode("created_at")] else None

        ordering = flat_attributes + entities_attributes + geo_attributes + user_attributes

        row_arr = []
        csv_arr = []

        for field in ordering:
            val = tweet_data[field] if tweet_data[field] else None

            if val is None:
                row_arr.append('\N')
            else:
                row_arr.append(unicode(val))

        for field in ["lat", "lng", "text"]:
            if field is "text":
                val = self.strip_str(str(tweet_data[field].encode('utf-8'))) if tweet_data[field] else None
            else:
                val = self.strip_str(tweet_data[field]) if tweet_data[field] else None

            if val is None:
                csv_arr.append('\007')
            else:
                csv_arr.append(unicode(val))

        data_str.write('\007'.join(row_arr).encode('utf-8') + '\n')
        csv_str.write(','.join(csv_arr).encode('utf-8') + '\n')

    def strip_str(self, string):
        if type(string) != type("string"):
            return string
        pattern = re.compile(r"[^@a-zA-Z0-9\s]")
        string = pattern.sub('', string)
        string = string.strip()
        return string

    def write_to_s3(self, data_str, csv_str, last_offset):

        mmddyy = datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%y')

        # Copy data for load to S3
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)
        s3_file = Key(bucket)
        s3_file.key = self.KAFKA_TOPIC + "/" + mmddyy + "/" + self.S3_KEY + "_offset_" + str(last_offset) + "_" + str(calendar.timegm(datetime.utcnow().timetuple())) + ".txt"
        data_str.seek(0)
        s3_file.set_contents_from_file(data_str)
        s3_file.key = self.KAFKA_TOPIC + "/" + mmddyy + "/" + self.S3_KEY + "_offset_" + str(last_offset) + "_" + str(calendar.timegm(datetime.utcnow().timetuple())) + ".csv"
        csv_str.seek(0)
        s3_file.set_contents_from_file(csv_str)
        s3_connection.close()

    def test_ts(self):

        kafka = KafkaClient(config.get("kafka.host1") + "," + config.get("kafka.host2"))

        # consumer = SimpleConsumer(kafka, "my-group112", "test")
        consumer = SimpleConsumer(kafka, self.GROUP_NAME, self.KAFKA_TOPIC,
                                  fetch_size_bytes=3000000, buffer_size=2000000000, max_buffer_size=2000000000)

        while True:
            print("HELLO")
            # Prepare data for insert and copy to S3
            # data_str = StringIO()
            count = 0
            # last_offset = 2

            consumer.seek(2, 0)

            for message in consumer.get_messages(count=100, block=False, timeout=0.1):
                count += 1

                print(message.message.value)

            #     # Write tweets to StringIO
            #     self.write_to_data_str(message, data_str)

            # # Store batch tweets to S3
            # self.write_to_s3(data_str, last_offset)

            if count != 100:
                break

if __name__ == "__main__":
    job = job()
    # job.test_ts()
    job.run()
    # CLAY_CONFIG=./config/local.yaml python -m jobs.kafka.twitter_processes.tweeter_stream_producer
