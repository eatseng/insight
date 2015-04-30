from clay import config
from datetime import datetime
from cassandra.cluster import Cluster

script_log = config.get_logger('monkey_job')


class job(object):

    TABLE_NAME = "tweet_rankings"

    CREATE_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS insight.%s (
                yymmdd varchar,
                word text,
                count int,
            PRIMARY KEY ((yymmdd), count)
        )
        """ % TABLE_NAME

    INSERT_DATA_QUERY = """INSERT INTO %s (yymmdd, word, count) VALUES ('%s', '%s', %s)"""

    CREATE_LOCATION_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS insight.test_location (
                yymmdd varchar,
                lat float,
                lng float,
                conference text,
                word text,
                count int,
            PRIMARY KEY ((yymmdd), lat, lng, conference, count))
        )
        """
    A = """SELECT * FROM insight.test_location WHERE yymmdd = '150127' AND lat in (1, 0, 2) AND lng > 5 AND lng < 10;"""
    B = """INSERT INTO insight.test_location (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces', 'best', 100);
       INSERT INTO insight.test_location (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces', 'great', 94);
       INSERT INTO insight.test_location (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces2', 'magnificent', 40);
       INSERT INTO insight.test_location (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces2', 'amazing', 9);
       INSERT INTO insight.test_location (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 20, 1, 'sec', 'super', 11);
       INSERT INTO insight.test_location (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 20, 1, 'sec', 'bad', 34);
       INSERT INTO insight.test_location (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 2, 20, 'gac', 'awesome', 56);
   """

    CREATE_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS insight.test_conf (
                yymmdd varchar,
                lat float,
                lng float,
                conference text,
                word text,
                count int,
            PRIMARY KEY ((yymmdd), conference, lat, lng, count))
        )
        """

    A = """SELECT * FROM insight.test_conf WHERE yymmdd = '150127' AND lat in (1, 0, 2) AND lng > 5 AND lng < 10;"""
    B = """INSERT INTO insight.test_conf (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces', 'best', 100);
       INSERT INTO insight.test_conf (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces', 'great', 94);
       INSERT INTO insight.test_conf (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces2', 'magnificent', 40);
       INSERT INTO insight.test_conf (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 1, 20, 'ces2', 'amazing', 9);
       INSERT INTO insight.test_conf (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 20, 1, 'sec', 'super', 11);
       INSERT INTO insight.test_conf (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 20, 1, 'sec', 'bad', 34);
       INSERT INTO insight.test_conf (yymmdd, lat, lng, conference, word, count) VALUES ('150127', 2, 20, 'gac', 'awesome', 56);
   """


    def run(self):

        cluster = Cluster([config.get("cassandra.host1"), config.get("cassandra.host2")])
        session = cluster.connect('insight')

        print(session.execute("""describe tables"""))

if __name__ == "__main__":
    job = job()
    job.run()
    # CLAY_CONFIG=./config/local.yaml python -m jobs.kafka.twitter_processes.etl
