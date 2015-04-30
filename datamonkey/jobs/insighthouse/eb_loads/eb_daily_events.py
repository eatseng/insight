import traceback
import sys

import calendar
from clay import config
from datetime import datetime, timedelta
from datamonkey.lib.eventbrite_rest import EventBriteRest
from datamonkey.lib.mysql import Mysql
from datamonkey.lib.redshift import Redshift
from datamonkey.lib.error import error_email
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from cStringIO import StringIO

reload(sys)
sys.setdefaultencoding("utf-8")
script_log = config.get_logger('monkey_job')


# class job(EtlJob):
class job(object):

    TABLE_NAME = 'eb_daily_events'

    TMP_TABLE_NAME = TABLE_NAME + "_tmp"

    CREATE_RS_TABLE_QUERY = """CREATE TABLE %s (
            eb_id VARCHAR(128),
            url VARCHAR(1024),
            logo_url VARCHAR(1024),
            event_name VARCHAR(1024),
            event_type VARCHAR(1024),
            start_time_utc TIMESTAMP,
            end_time_utc TIMESTAMP,
            eb_created_at TIMESTAMP,
            eb_updated_at TIMESTAMP,
            capacity INT,
            online_event BOOLEAN,
            venue_id INT,
            venue_name VARCHAR(1024),
            latitude FLOAT,
            longitude FLOAT,
            category VARCHAR(256),
            created_at TIMESTAMP,
            PRIMARY KEY(eb_id)
        )
        DISTSTYLE ALL
        SORTKEY(end_time_utc);"""

    LAST_VALUE = "eb_id"

    CREATE_TABLE_QUERY = """CREATE TABLE %s (
            id INT NOT NULL,
            eb_id VARCHAR(128) NOT NULL,
            url VARCHAR(1024) NULL,
            logo_url VARCHAR(1024) NULL,
            event_name VARCHAR(1024) NULL,
            event_type VARCHAR(1024) NULL,
            start_time_utc datetime NOT NULL,
            end_time_utc datetime NOT NULL,
            eb_created_at datetime NULL,
            eb_updated_at datetime NULL,
            capacity INT NULL,
            online_event BOOLEAN NULL,
            venue_id INT NULL,
            venue_name VARCHAR(1024) NULL,
            latitude FLOAT(8, 4) NOT NULL,
            longitude FLOAT(8, 4) NOT NULL,
            category VARCHAR(128) NOT NULL,
            created_at datetime NOT NULL,
            PRIMARY KEY (id)
        );
        """

    ADD_INDEX_QUERY1 = """CREATE INDEX start_time ON eb_daily_events (start_time_utc);"""
    ADD_INDEX_QUERY2 = """CREATE INDEX latitude ON eb_daily_events (latitude);"""
    ADD_INDEX_QUERY3 = """CREATE INDEX longitude ON eb_daily_events (longitude);"""

    LAST_VALUE_QUERY = """SELECT MAX(id), MAX(created_at) FROM %s
        """ % TABLE_NAME

    MYSQL = Mysql()

    REDSHIFT = Redshift()

    S3_SUBDIRECTORY = "eventbrite/"

    S3_KEY = S3_SUBDIRECTORY + TABLE_NAME + "_" + str(calendar.timegm(datetime.utcnow().timetuple())) + ".txt"

    S3_BKP_KEY = S3_SUBDIRECTORY + "true_data/" + TABLE_NAME + "_" + str(calendar.timegm(datetime.utcnow().timetuple())) + ".txt"

    EVENT_TYPES = ["software", "wearable", "fashion", "data science", "smart home", "3d printing",
                   "autonomous vehicle", "consumer electronics", "bit coin", "solar", "big data",
                   "superbowl", "football", "art", "music"
                   ]

    S3_PATH = "s3://%s/%s" % (config.get('S3.bucket'), S3_KEY)

    def run(self, options=None):
        # self.create_run_flag()
        # start_time = datetime.utcnow()

        script_log.info("Running job: %s" % self.TABLE_NAME)

        # today = datetime.utcnow().date()
        today = datetime.now().date()

        # try:

        # RECREATE TABLE
        # mysql.drop_table(self.TABLE_NAME)

        # Create table if it doesn't exist in the database
        # if self.MYSQL.if_table_exists(self.TABLE_NAME) is False:
        #     self.MYSQL.execute(self.CREATE_TABLE_QUERY % self.TABLE_NAME)
        #     self.MYSQL.execute(self.ADD_INDEX_QUERY1)
        #     self.MYSQL.execute(self.ADD_INDEX_QUERY2)
        #     self.MYSQL.execute(self.ADD_INDEX_QUERY3)

        # Drop unncessary table
        if self.REDSHIFT.if_table_exists(self.TMP_TABLE_NAME) is True:
            self.REDSHIFT.drop_table(self.TMP_TABLE_NAME)

        # Create table if it doesn't exist in the database
        if self.REDSHIFT.if_table_exists(self.TABLE_NAME) is False:
            self.REDSHIFT.execute(self.CREATE_RS_TABLE_QUERY % self.TABLE_NAME)

        # Pull data from EventBrite
        event_brite_data = self.get_list_of_events(today, self.EVENT_TYPES)

        # Load data into Mysql
        # self.load_data_to_mysql(event_brite_data)

        # Load data into S3
        self.load_data_to_s3(event_brite_data)

        # Back up data into S3
        self.backup_data_to_s3(event_brite_data)

        # Create temporary table
        self.REDSHIFT.execute(self.CREATE_RS_TABLE_QUERY % self.TMP_TABLE_NAME)

        # Load data from S3 to temp table
        self.REDSHIFT.load_s3(self.S3_PATH, self.TMP_TABLE_NAME)

        # Combine tables
        self.REDSHIFT.upsert(self.TMP_TABLE_NAME, self.TABLE_NAME, self.LAST_VALUE)

        # Clean up
        self.REDSHIFT.drop_table(self.TMP_TABLE_NAME)
        self.clean_s3_files()

        # Permissions
        # self.REDSHIFT.readonly_select(self.TABLE_NAME)

        # duration = int((datetime.utcnow() - start_time).total_seconds())
        # self.remove_run_flag()
        # self.log_success(duration)

        # except Exception, e:
        #     script_log.error("Job %s failed: %s" % (self.TABLE_NAME, e))
        #     # self.remove_run_flag()

        #     # if options is not None and options.get('recreate', None) is not None and options['recreate']:
        #     #     cleanup_s3_file(s3_key)
        #     #     db.drop_table(tmp_table_name)
        #     # else:
        #     #     cleanup_s3_file(s3_key)

        #     error_email(table_name=self.TABLE_NAME, error=e, trace=(''.join(traceback.format_stack())))

    def get_list_of_events(self, date, event_types):

        data = {}

        eventbrite = EventBriteRest(rest_endpoint=config.get("eventbrite.endpoint_rest"),
                                    token=config.get("eventbrite.token")
                                    )

        start_time = (date + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
        end_time = (date + timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Search for each event types
        for event_type in event_types:
            result = eventbrite.search_events_by(event_type, start_time, end_time, 1)

            # Determine the number of events returned from api
            event_count = result["pagination"]["object_count"]
            page_count = result["pagination"]["page_count"]

            # If there is data
            if event_count > 0:
                data[event_type] = []

                # Get data from each page
                for i in xrange(1, page_count + 1):

                    result = eventbrite.search_events_by(event_type, start_time, end_time, i)

                    # Append data into array
                    for event in result["events"]:
                        data[event_type].append(event)

        return data

    def load_data_to_mysql(self, data):
        insert_row_query = """INSERT INTO %s VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """ % (self.TABLE_NAME)

        last_id, last_created_at = self.MYSQL.select(self.LAST_VALUE_QUERY)[0]

        last_id = int(last_id + 1) if last_id is not None else 1

        # Get all data from all given category
        for key in data:
            for event in data[key]:
                eb_id = event[unicode("id")].encode("utf-8") if event[unicode("id")] else 0
                url = event[unicode("url")].encode("utf-8") if event[unicode("url")] else None
                logo_url = event[unicode("logo_url")].encode("utf-8") if event[unicode("logo_url")] else None
                event_name = event[unicode("name")][unicode("text")].encode("utf-8") if event[unicode("name")] and event[unicode("name")][unicode("text")] else None
                event_type = event[unicode("format")][unicode("name_localized")].encode("utf-8") if event[unicode("format")] and unicode("name_localized") in event[unicode("format")] else None
                start_time_utc = datetime.strptime(event[unicode("start")][unicode("utc")], '%Y-%m-%d %H:%M:%S')
                end_time_utc = datetime.strptime(event[unicode("end")][unicode("utc")], '%Y-%m-%d %H:%M:%S')
                ev_created_at = datetime.strptime(event[unicode("created")], '%Y-%m-%d %H:%M:%S')
                ev_updated_at = datetime.strptime(event[unicode("changed")], '%Y-%m-%d %H:%M:%S')
                capacity = int(event[unicode("capacity")])
                online_event = event[unicode("online_event")].encode("utf-8") if event[unicode("online_event")] else False
                venue_id = int(event[unicode("venue_id")]) if event[unicode("venue_id")] else -1
                venue_name = event[unicode("venue")][unicode("name")].encode("utf-8") if event[unicode("venue")] and unicode("name") in event[unicode("venue")] and event[unicode("venue")][unicode("name")] else None
                latitude = float(event[unicode("venue")][unicode("latitude")]) if event[unicode("venue")] and unicode("latitude") in event[unicode("venue")] else 0
                longitude = float(event[unicode("venue")][unicode("longitude")]) if event[unicode("venue")] and unicode("longitude") in event[unicode("venue")] else 0
                category = key.encode("utf-8")
                created_at = datetime.utcnow()

                mysql_row_data = [last_id, eb_id, url, logo_url, event_name, event_type, start_time_utc, end_time_utc, ev_created_at, ev_updated_at, capacity, online_event, venue_id, venue_name, latitude, longitude, category, created_at]

                # Insert row into mysql database
                self.MYSQL.execute(insert_row_query, mysql_row_data)

                last_id += 1

    def load_data_to_s3(self, data):

        # Prepare data for insert and copy to S3
        data_str = StringIO()

        ordering = ["eb_id", "url", "logo_url", "event_name", "event_type", "start_time_utc", "end_time_utc", "ev_created_at", "ev_updated_at", "capacity", "online_event", "venue_id", "venue_name", "latitude", "longitude", "category", "created_at"]

        # Get all data from all given category
        for key in data:
            for event in data[key]:

                event_data = {}

                event_data["eb_id"] = event[unicode("id")].encode("utf-8") if event[unicode("id")] else 0
                event_data["url"] = event[unicode("url")].encode("utf-8") if event[unicode("url")] else None
                event_data["logo_url"] = event[unicode("logo_url")].encode("utf-8") if event[unicode("logo_url")] else None
                event_data["event_name"] = event[unicode("name")][unicode("text")].strip().encode("utf-8") if event[unicode("name")] and event[unicode("name")][unicode("text")] else None
                event_data["event_type"] = event[unicode("format")][unicode("name_localized")].encode("utf-8") if event[unicode("format")] and unicode("name_localized") in event[unicode("format")] else None
                event_data["start_time_utc"] = datetime.strptime(event[unicode("start")][unicode("utc")], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                event_data["end_time_utc"] = datetime.strptime(event[unicode("end")][unicode("utc")], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                event_data["ev_created_at"] = datetime.strptime(event[unicode("created")], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                event_data["ev_updated_at"] = datetime.strptime(event[unicode("changed")], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                event_data["capacity"] = int(event[unicode("capacity")]) if event[unicode("capacity")] else 0
                event_data["online_event"] = event[unicode("online_event")].encode("utf-8") if event[unicode("online_event")] else False
                event_data["venue_id"] = int(event[unicode("venue_id")]) if event[unicode("venue_id")] else -1
                event_data["venue_name"] = event[unicode("venue")][unicode("name")].encode("utf-8") if event[unicode("venue")] and unicode("name") in event[unicode("venue")] and event[unicode("venue")][unicode("name")] else None
                event_data["latitude"] = float(event[unicode("venue")][unicode("latitude")]) if event[unicode("venue")] and unicode("latitude") in event[unicode("venue")] else 0
                event_data["longitude"] = float(event[unicode("venue")][unicode("longitude")]) if event[unicode("venue")] and unicode("longitude") in event[unicode("venue")] else 0
                event_data["category"] = key.encode("utf-8")
                event_data["created_at"] = datetime.utcnow()

                row_arr = []

                for field in ordering:
                    val = event_data[field] if event_data[field] else None
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

    def backup_data_to_s3(self, data):

        # Prepare data for insert and copy to S3
        data_str = StringIO()

        event_ordering = ["resource_uri", "id", "url", "logo_url", "created", "changed", "capacity", "status", "organizer_id", "venue_id", "category_id", "subcategory_id", "format_id"]

        # Get all data from all given category
        for key in data:
            print(key)
            # For an event in events of that category
            for event in data[key]:

                event_data = {}

                for order in event_ordering:
                    if type(event[unicode(order)]) == unicode:
                        event_data[order] = event[unicode(order)].encode("utf-8") if event[unicode(order)] else None
                    elif type(event[unicode(order)]) == int:
                        event_data[order] = str(event[unicode(order)]).encode("utf-8") if event[unicode(order)] else "-1"
                    else:
                        event_data[order] = None

                event_data["name"] = event[unicode("name")][unicode("text")].encode("utf-8") if event[unicode("name")] and event[unicode("name")][unicode("text")] else None
                event_data["description"] = event[unicode("description")][unicode("text")].encode("utf-8") if event[unicode("description")] and event[unicode("description")][unicode("text")] else None
                event_data["start"] = event[unicode("start")][unicode("utc")].encode("utf-8") if event[unicode("start")] and event[unicode("start")][unicode("utc")] else None
                event_data["end"] = event[unicode("end")][unicode("utc")].encode("utf-8") if event[unicode("end")] and event[unicode("end")][unicode("utc")] else None
                event_data["organizer_name"] = event[unicode("organizer")][unicode("name")].encode("utf-8") if event[unicode("organizer")] and event[unicode("organizer")][unicode("name")] else None
                event_data["organizer_description"] = event[unicode("organizer")][unicode("description")][unicode("text")].encode("utf-8") if event[unicode("organizer")] and event[unicode("organizer")][unicode("description")] and event[unicode("organizer")][unicode("description")][unicode("text")] else None
                event_data["organizer_num_past_events"] = str(event[unicode("organizer")][unicode("num_past_events")]).encode("utf-8") if event[unicode("organizer")] and event[unicode("organizer")][unicode("num_past_events")] else None
                event_data["category"] = event[unicode("category")][unicode("name_localized")].encode("utf-8") if event[unicode("category")] and event[unicode("category")][unicode("name_localized")] else None
                event_data["subcategory"] = event[unicode("subcategory")][unicode("name")].encode("utf-8") if event[unicode("subcategory")] and event[unicode("subcategory")][unicode("name")] else None
                event_data["format"] = event[unicode("format")][unicode("name_localized")].encode("utf-8") if event[unicode("format")] and event[unicode("format")][unicode("name_localized")] else None
                event_data["venue_name"] = event[unicode("venue")][unicode("name")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("name")] else None
                event_data["venue_address1"] = event[unicode("venue")][unicode("address")][unicode("address_1")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("address")] and event[unicode("venue")][unicode("address")][unicode("address_1")] else None
                event_data["venue_address2"] = event[unicode("venue")][unicode("address")][unicode("address_2")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("address")] and event[unicode("venue")][unicode("address")][unicode("address_2")] else None
                event_data["venue_city"] = event[unicode("venue")][unicode("address")][unicode("city")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("address")] and event[unicode("venue")][unicode("address")][unicode("city")] else None
                event_data["venue_region"] = event[unicode("venue")][unicode("address")][unicode("region")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("address")] and event[unicode("venue")][unicode("address")][unicode("region")] else None
                event_data["venue_postal_code"] = event[unicode("venue")][unicode("address")][unicode("postal_code")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("address")] and event[unicode("venue")][unicode("address")][unicode("postal_code")] else None
                event_data["venue_country"] = event[unicode("venue")][unicode("address")][unicode("country")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("address")] and event[unicode("venue")][unicode("address")][unicode("country")] else None
                event_data["venue_latitude"] = event[unicode("venue")][unicode("latitude")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("latitude")] else None
                event_data["venue_longitude"] = event[unicode("venue")][unicode("longitude")].encode("utf-8") if event[unicode("venue")] and event[unicode("venue")][unicode("longitude")] else None
                event_data["category"] = key.encode("utf-8")

                ordering = event_ordering + ["name", "description", "start", "end", "organizer_name", "organizer_description",
                                             "organizer_num_past_events", "category", "subcategory", "format", "venue_name", "venue_address1",
                                             "venue_address2", "venue_city", "venue_region", "venue_postal_code", "venue_country", "venue_latitude",
                                             "venue_longitude", "category"]

                row_arr = []

                for field in ordering:
                    val = event_data[field] if event_data[field] else None
                    if val is None:
                        row_arr.append('\N')
                    else:
                        row_arr.append(unicode(val))

                data_str.write('\007'.join(row_arr).encode('utf-8') + '\n')

        # Copy data for load to S3
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)
        s3_file = Key(bucket)
        s3_file.key = self.S3_BKP_KEY
        data_str.seek(0)
        s3_file.set_contents_from_file(data_str)

    def clean_s3_files(self):
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)

        for key in bucket.list(self.S3_KEY):
            bucket.delete_key(key)

    def test_s3(self, options=None):

        s3_key = "eventbrite/" + self.TABLE_NAME + "_" + str(calendar.timegm(datetime.utcnow().timetuple())) + ".txt"

        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)

        line_buff = StringIO()
        line_buff.write("Hello World MORE AND MORE STUFF")

        s3_file = Key(bucket)
        s3_file.key = s3_key
        line_buff.seek(0)
        s3_file.set_contents_from_file(line_buff)

if __name__ == "__main__":
    job = job()
    # job.test_eb()
    # job.test_s3()
    job.run()
    # CLAY_CONFIG=./config/local.yaml python -m jobs.insighthouse.eb_loads.eb_daily_events
