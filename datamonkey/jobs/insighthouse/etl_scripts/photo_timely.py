import calendar
import operator

from clay import config
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from boto.s3.connection import S3Connection
from datamonkey.lib.redshift import Redshift
from datamonkey.lib.flickr_rest import FlickrRest
from boto.s3.key import Key
from cStringIO import StringIO

script_log = config.get_logger('monkey_job')


class job(object):

    TABLE_NAME = "keyword_photos"

    TMP_TABLE_NAME = TABLE_NAME + "_tmp"

    LAST_VALUE = "pid"

    # Decimal point of gps coordinate
    PRECISION = "2"

    CREATE_TABLE = """CREATE TABLE %s (
            pid INT8,
            yymmddhh VARCHAR(32),
            word VARCHAR(128),
            url VARCHAR(512),
            PRIMARY KEY (word)
        )
        DISTSTYLE EVEN
        SORTKEY(yymmddhh);
        """

    QUERY_RS_EVENTS = """SELECT DISTINCT
            eb_id,
            event_name,
            event_type,
            start_time_utc,
            end_time_utc,
            latitude,
            longitude
        FROM eb_daily_events
        WHERE end_time_utc > GETDATE() - INTERVAL '7 day'
    """

    GET_MAX_ID = """SELECT
                MAX(pid)
            FROM %s
        """ % (TABLE_NAME)

    CLEAN_UP = """DELETE FROM %s WHERE yymmddhh = '%s'"""

    QUERY_TODAYS_TWEET = """
        SELECT * FROM insight.conference_batch_details WHERE yymmddhh = '%s'
    """

    CASSANDRA = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2")]).connect('insight')

    REDSHIFT = Redshift()

    S3_KEY = TABLE_NAME + "_" + str(calendar.timegm(datetime.utcnow().timetuple())) + ".txt"

    S3_PATH = "s3://%s/%s" % (config.get('S3.bucket'), S3_KEY)

    def run(self):

        # Make sure tmp table does not exist
        if self.REDSHIFT.if_table_exists(self.TMP_TABLE_NAME) is True:
            self.REDSHIFT.drop_table(self.TMP_TABLE_NAME)

        # Create table if it doesn't exist in the database
        if self.REDSHIFT.if_table_exists(self.TABLE_NAME) is False:
            self.REDSHIFT.execute(self.CREATE_TABLE % self.TABLE_NAME)

        (last_id) = self.REDSHIFT.select(self.GET_MAX_ID)[0][0]
        last_id = last_id if last_id else 0

        today = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        # Get spark mr data from Cassandra
        tweet_data = self.get_tweets(today)

        if len(tweet_data.keys()) is 0:

            last_hour = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + (datetime.utcnow() - timedelta(hours=1)).strftime('%H')
            # Get spark mr data from Cassandra
            tweet_data = self.get_tweets(last_hour)

        # Precompute event statistics for conferences
        photo_data = self.fetch_photos_for_conference(tweet_data, last_id)

        # Write data to S3
        self.load_data_to_s3(photo_data)

        # Create temporary table
        self.REDSHIFT.execute(self.CREATE_TABLE % self.TMP_TABLE_NAME)

        # Load data from S3 to temp table
        self.REDSHIFT.load_s3(self.S3_PATH, self.TMP_TABLE_NAME)

        # Combine tables
        self.REDSHIFT.upsert(self.TMP_TABLE_NAME, self.TABLE_NAME, self.LAST_VALUE)

        # Clean up
        self.REDSHIFT.drop_table(self.TMP_TABLE_NAME)
        self.clean_s3_files(self.S3_KEY)
        self.clean_up_old_photos()

    def fetch_photos_for_conference(self, word_data, last_id):

        bag_of_words = []

        photo_data = []

        flickr = FlickrRest(rest_endpoint=config.get("flickr.endpoint_rest"),
                            api_key=config.get("flickr.api_key")
                            )

        yymmddhh = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        db_data = self.REDSHIFT.select(self.QUERY_RS_EVENTS)

        for row in db_data:

            if row["latitude"] is None and row["longitude"] is None:
                continue

            location = ("%.2f" % row["latitude"]) + "," + ("%.2f" % row["longitude"])

            if location not in word_data:
                continue

            for word, count in word_data[location].iteritems():
                if word == "OVERALL_CNT" or word in bag_of_words:
                    continue

                if len(word) is 0:
                    continue

                for url in flickr.get_photos_by_keyword(word):

                    # Increment last_id
                    last_id += 1

                    data = {}

                    data["pid"] = last_id
                    data["yymmddhh"] = yymmddhh
                    data["word"] = word
                    data["url"] = url

                    photo_data.append(data)

                bag_of_words.append(word)

        return photo_data

    def load_data_to_s3(self, photo_data):

        data_str = StringIO()

        ordering = ["pid", "yymmddhh", "word", "url"]

        for data in photo_data:

            row_arr = []

            for field in ordering:
                val = data[field] if data[field] else None

                if val is None:
                    row_arr.append('\N')
                else:
                    row_arr.append(unicode(val))

            data_str.write('\007'.join(row_arr).encode('utf-8') + '\n')

        # Copy data for load to S3
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)
        s3_file = Key(bucket)
        s3_file.key = self.S3_KEY
        data_str.seek(0)
        s3_file.set_contents_from_file(data_str)

    def get_tweets(self, timestamp):

        word_data, sorted_by_loc = {}, []

        print(self.QUERY_TODAYS_TWEET % timestamp)

        tweeted_words = self.CASSANDRA.execute(self.QUERY_TODAYS_TWEET % timestamp)
        # tweeted_words = self.CASSANDRA.execute(self.QUERY_TODAYS_TWEET % '15020221')

        for (yymmddhh, location, string, data, timestamp) in tweeted_words:

            if location not in word_data:
                word_data[location] = {}

            # for pair in word_count_pairs:
            for word, count in data.iteritems():

                word = word.encode("utf-8")

                if word not in word_data[location]:
                    word_data[location][word] = count
                else:
                    word_data[location][word] += count

        for location in word_data:
            # In place sort by number of words in desc order
            sorted_by_loc = sorted(word_data[location].items(), key=operator.itemgetter(1))
            sorted_by_loc.reverse()

            # Re-enter sorted data
            word_data[location] = {}
            for word_pair in sorted_by_loc:
                word = word_pair[0]
                count = word_pair[1]
                word_data[location][word] = count

        return word_data

    def clean_s3_files(self, s3_key):
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)

        for key in bucket.list(s3_key):
            bucket.delete_key(key)

    def clean_up_old_photos(self):

        last_week = datetime.utcnow() - timedelta(days=7)

        yymmddhh = last_week.strftime('%y') + last_week.strftime('%m') + last_week.strftime('%d') + last_week.strftime('%H')

        self.REDSHIFT.execute(self.CLEAN_UP % (self.TABLE_NAME, yymmddhh))

if __name__ == "__main__":
    job = job()
    job.run()
    # CLAY_CONFIG=./config/local.yaml python -m jobs.kafka.twitter_processes.etl
