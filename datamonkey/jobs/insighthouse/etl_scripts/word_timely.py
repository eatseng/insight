import calendar

from clay import config
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
from boto.s3.connection import S3Connection
from datamonkey.lib.redshift import Redshift
from boto.s3.key import Key
from cStringIO import StringIO

script_log = config.get_logger('monkey_job')


class job(object):

    TABLE_NAME = "time_acc_words"

    TMP_TABLE_NAME = TABLE_NAME + "_tmp"

    LAST_VALUE = "wid"

    CREATE_TABLE = """CREATE TABLE %s (
            wid INT8,
            words VARCHAR(256),
            latitude FLOAT,
            longitude FLOAT,
            count INT,
            created_at TIMESTAMP,
            PRIMARY KEY (latitude, longitude)
        )
        DISTSTYLE EVEN
        SORTKEY(created_at);
        """

    GET_MAX_ID = """SELECT
                MAX(wid)
            FROM %s
        """ % (TABLE_NAME)

    QUERY_TODAYS_TWEET = """SELECT * FROM insight.conference_batch_details WHERE yymmddhh = '%s'"""

    DELETE_TABLE = """DELETE FROM insight.conference_details WHERE yymmddhh = '%s'"""

    CASSANDRA = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2"), config.get("cassandra.host3")]).connect('insight')

    REDSHIFT = Redshift()

    S3_KEY = TABLE_NAME + "_" + str(calendar.timegm(datetime.utcnow().timetuple())) + ".txt"

    S3_PATH = "s3://%s/%s" % (config.get('S3.bucket'), S3_KEY)

    def run(self):

        # Create table if it doesn't exist in the database
        if self.REDSHIFT.if_table_exists(self.TABLE_NAME) is False:
            self.REDSHIFT.execute(self.CREATE_TABLE % self.TABLE_NAME)

        (last_id) = self.REDSHIFT.select(self.GET_MAX_ID)[0][0]
        last_id = last_id if last_id else 0

        # Get spark mr data from Cassandra and load to s3
        self.get_and_load_words_to_s3(last_id)

        # Create temporary table
        self.REDSHIFT.execute(self.CREATE_TABLE % self.TMP_TABLE_NAME)

        # Load data from S3 to temp table
        self.REDSHIFT.load_s3(self.S3_PATH, self.TMP_TABLE_NAME)

        # Combine tables
        self.REDSHIFT.upsert(self.TMP_TABLE_NAME, self.TABLE_NAME, self.LAST_VALUE)

        # Clean up
        self.REDSHIFT.drop_table(self.TMP_TABLE_NAME)
        self.clean_s3_files(self.S3_KEY)
        self.delete_past_table()

    def get_and_load_words_to_s3(self, i):

        data_str = StringIO()

        # ordering = ["wid", "words", "latitude", "longitude", "count", "created_at"]

        timestamp = datetime.utcnow().strftime('%y') + datetime.utcnow().strftime('%m') + datetime.utcnow().strftime('%d') + datetime.utcnow().strftime('%H')

        tweeted_words = self.CASSANDRA.execute(self.QUERY_TODAYS_TWEET % timestamp)
        # tweeted_words = self.CASSANDRA.execute(self.QUERY_TODAYS_TWEET % '15013006')

        for (yymmddhh, location, string, data, timestamp) in tweeted_words:

            loc = location.split(",")
            lat = float(loc[0])
            lng = float(loc[1])

            # word_count_pairs = [pair for pair in string.split(":")]
            for word, count in data.iteritems():

                i += 1
                row_arr = []

                word = word.encode("utf-8")

                if word == "OVERALL_CNT":
                    continue

                row_arr = [unicode(i), unicode(word), unicode(lat), unicode(lng), unicode(count), unicode(datetime.utcnow())]

                data_str.write('\007'.join(row_arr).encode('utf-8') + '\n')

        # Copy data for load to S3
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)
        s3_file = Key(bucket)
        s3_file.key = self.S3_KEY
        data_str.seek(0)
        s3_file.set_contents_from_file(data_str)

    def delete_past_table(self):

        yesterday = datetime.utcnow() - timedelta(days=1)

        timestamp = yesterday.strftime('%y') + yesterday.strftime('%m') + yesterday.strftime('%d') + yesterday.strftime('%H')

        # Delete past record
        self.CASSANDRA.execute(self.DELETE_TABLE % timestamp)

    def clean_s3_files(self, s3_key):
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)

        for key in bucket.list(s3_key):
            bucket.delete_key(key)

if __name__ == "__main__":
    job = job()
    job.run()
    # CLAY_CONFIG=./config/local.yaml python -m jobs.kafka.twitter_processes.etl
