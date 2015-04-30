import calendar
import operator

from clay import config
from datetime import datetime
from cassandra.cluster import Cluster
from boto.s3.connection import S3Connection
from datamonkey.lib.redshift import Redshift
from boto.s3.key import Key
from cStringIO import StringIO

script_log = config.get_logger('monkey_job')


class job(object):

    TABLE_NAME = "time_acc_conferences"

    TMP_TABLE_NAME = TABLE_NAME + "_tmp"

    # LAST_VALUE = "tid"

    # Decimal point of gps coordinate
    PRECISION = "2"

    CREATE_TABLE = """CREATE TABLE %s (
            tid INT,
            eb_id VARCHAR(128),
            event_name VARCHAR(1024),
            event_type VARCHAR(1024),
            latitude FLOAT,
            longitude FLOAT,
            start_time_utc TIMESTAMP,
            end_time_utc TIMESTAMP,
            tweets INT,
            created_at TIMESTAMP,
            word1 VARCHAR(128) DEFAULT NULL,
            word2 VARCHAR(128) DEFAULT NULL,
            word3 VARCHAR(128) DEFAULT NULL,
            word4 VARCHAR(128) DEFAULT NULL,
            word5 VARCHAR(128) DEFAULT NULL,
            word6 VARCHAR(128) DEFAULT NULL,
            word7 VARCHAR(128) DEFAULT NULL,
            word8 VARCHAR(128) DEFAULT NULL,
            word9 VARCHAR(128) DEFAULT NULL,
            word10 VARCHAR(128) DEFAULT NULL,
            count1 INT DEFAULT NULL,
            count2 INT DEFAULT NULL,
            count3 INT DEFAULT NULL,
            count4 INT DEFAULT NULL,
            count5 INT DEFAULT NULL,
            count6 INT DEFAULT NULL,
            count7 INT DEFAULT NULL,
            count8 INT DEFAULT NULL,
            count9 INT DEFAULT NULL,
            count10 INT DEFAULT NULL,
            PRIMARY KEY (end_time_utc, eb_id)
        )
        DISTSTYLE ALL
        SORTKEY(end_time_utc);
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
                MAX(tid)
            FROM %s
        """ % (TABLE_NAME)

    QUERY_TODAYS_TWEET = """
        SELECT * FROM insight.conference_batch_details WHERE yymmddhh = '%s'
    """

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

        # Get spark mr data from Cassandra
        tweet_data = self.get_todays_tweet()

        # Precompute event statistics for conferences
        stats_data = self.compute_conference_statistics(tweet_data, last_id)

        # # Write data to S3
        self.load_data_to_s3(stats_data)

        # # Create temporary table
        self.REDSHIFT.execute(self.CREATE_TABLE % self.TMP_TABLE_NAME)

        # # Load data from S3 to temp table
        self.REDSHIFT.load_s3(self.S3_PATH, self.TMP_TABLE_NAME)

        # # Combine tables
        self.REDSHIFT.upsert(self.TMP_TABLE_NAME, self.TABLE_NAME, "eb_id")

        # # Clean up
        self.REDSHIFT.drop_table(self.TMP_TABLE_NAME)
        self.clean_s3_files(self.S3_KEY)

    def compute_conference_statistics(self, word_data, last_id):

        stats_data = []

        db_data = self.REDSHIFT.select(self.QUERY_RS_EVENTS)

        i = last_id + 1
        for row in db_data:

            data = {}

            if row["latitude"] is None and row["longitude"] is None:
                continue

            location = ("%.2f" % row["latitude"]) + "," + ("%.2f" % row["longitude"])

            if location not in word_data:
                continue

            cnt = 1
            for word, count in word_data[location].iteritems():
                if word == "OVERALL_CNT":
                    continue
                data["word" + str(cnt)] = word
                data["count" + str(cnt)] = count
                cnt += 1

            data["tid"] = i
            data["eb_id"] = str(row["eb_id"])
            data["event_name"] = row["event_name"]
            data["event_type"] = row["event_type"]
            data["latitude"] = float(row["latitude"])
            data["longitude"] = float(row["longitude"])
            data["start_time_utc"] = row["start_time_utc"]
            data["end_time_utc"] = row["end_time_utc"]
            data["tweets"] = word_data[location]["OVERALL_CNT"]
            data["created_at"] = datetime.utcnow()

            stats_data.append(data)

        return stats_data

    def load_data_to_s3(self, stats_data):

        data_str = StringIO()

        ordering = ["tid", "eb_id", "event_name", "event_type", "latitude", "longitude", "start_time_utc", "end_time_utc", "tweets", "created_at"]

        for data in stats_data:

            row_arr = []

            for field in ordering:
                val = data[field] if data[field] else None

                if val is None:
                    row_arr.append('\N')
                else:
                    row_arr.append(unicode(val))

            for data_type in ["word", "count"]:
                for i in xrange(10):
                    key = data_type + str(i + 1)

                    if key not in data:
                        row_arr.append(unicode('\N'))
                    else:
                        row_arr.append(unicode(data[key]))

            data_str.write('\007'.join(row_arr).encode('utf-8') + '\n')

        # Copy data for load to S3
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)
        s3_file = Key(bucket)
        s3_file.key = self.S3_KEY
        data_str.seek(0)
        s3_file.set_contents_from_file(data_str)

    def get_todays_tweet(self):

        word_data, sorted_by_loc = {}, []

        timestamp = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        cassandra = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2"), config.get("cassandra.host3")]).connect('insight')

        tweeted_words = cassandra.execute(self.QUERY_TODAYS_TWEET % timestamp)
        # tweeted_words = self.CASSANDRA.execute(self.QUERY_TODAYS_TWEET % '15020221')

        # for (yymmddhh, timestamp, data, lat, lng) in tweeted_words:
        for (yymmddhh, location, string, data, timestamp) in tweeted_words:

            if location not in word_data:
                word_data[location] = {}

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

if __name__ == "__main__":
    job = job()
    job.run()
    # CLAY_CONFIG=./config/local.yaml python -m jobs.kafka.twitter_processes.etl
