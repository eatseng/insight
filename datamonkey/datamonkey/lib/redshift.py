from __future__ import absolute_import

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from cStringIO import StringIO

from boto.s3.key import Key
from clay import config

from boto.s3.connection import S3Connection

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


class Redshift(object):

    ROW_DELIMITER = '\n'
    COL_DELIMITER = '\007'

    def __init__(self):
        self.conn_opts = dict(
            host=config.get("redshift_db.host"),
            port=config.get("redshift_db.port"),
            user=config.get("redshift_db.user"),
            password=config.get("redshift_db.password"),
            database=config.get("redshift_db.db")
        )

    def get_conn(self, dict_cursor=False):
        if dict_cursor is True:
            self.conn_opts['cursor_factory'] = psycopg2.extras.DictCursor
        return psycopg2.connect(**self.conn_opts)

    def execute(self, select_sql, data=[]):
        # print(select_sql)
        conn = self.get_conn(False)
        cursor = conn.cursor()
        cursor.execute(select_sql, data)
        cursor.connection.commit()
        conn.close()

    def select(self, select_sql):
        # print(select_sql)
        conn = self.get_conn(dict_cursor=True)
        cursor = conn.cursor()
        cursor.execute(select_sql)
        result = cursor.fetchall()
        conn.close()

        if result is None:
            return None
        else:
            return result

    def if_table_exists(self, table_name):
        query = """SELECT count(*)
                    FROM information_schema.tables
                    WHERE (table_name = %s);"""

        cursor = self.get_conn().cursor()
        cursor.execute(query, (table_name,))

        result = cursor.fetchall()

        cursor.connection.close()

        if result[0][0] > 0:
            return True
        else:
            return False

    def last_value(self, table_name, lv_col, ts_col=None):

        cursor = self.get_conn().cursor()

        if ts_col is None:
            query = """SELECT MAX(%s) FROM %s;""" % (lv_col, table_name)
        else:
            query = """SELECT
                            MAX(%s),
                            MAX(%s)
                        FROM %s
                        JOIN
                        (SELECT MAX(%s) AS ts_col FROM %s) AS sub
                        ON sub.ts_col = %s.%s;
                    """ % (lv_col, ts_col, table_name, ts_col, table_name, table_name, ts_col)

        cursor.execute(query)

        result = cursor.fetchall()

        cursor.connection.close()

        if result is None:
            return None, None
        else:
            if ts_col is None:
                return result[0][0], None
            else:
                return result[0][0], result[0][1]

    def select_s3(self, s3_key, select_sql, batch_size=None):
        s3_connection = S3Connection(config.get('S3.access_key'), config.get('S3.secret'))
        bucket = s3_connection.get_bucket(config.get('S3.bucket'), validate=False)

        fp = StringIO()

        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(select_sql)

        ordering = map(lambda c: c[0], cursor.description)

        row_count = 0
        last_row = []

        for row in cursor:
            row_arr = []
            for val in row:
                if val is None:
                    row_arr.append('\N')
                else:
                    row_arr.append(unicode(val))

            str_row = (self.COL_DELIMITER.join(row_arr).replace(self.ROW_DELIMITER, '') + self.ROW_DELIMITER).encode('utf-8')
            fp.write(str_row)

            row_count = row_count + 1
            last_row = row
            if batch_size is not None and row_count >= batch_size:
                break

        s3_file = Key(bucket)
        s3_file.key = s3_key
        fp.seek(0)
        s3_file.set_contents_from_file(fp)

        conn.close()

        if batch_size is None:
            return None, None
        else:
            return row_count, dict(zip(ordering, last_row))

    def load_s3(self, s3_path, table_name):
        query = """COPY %s FROM '%s' CREDENTIALS '%s' DELIMITER '%s' MAXERROR 0;
                """ % (table_name, s3_path, get_s3_credentials(), self.COL_DELIMITER)

        self.execute(query)

    def upsert(self, tmp_table_name, table_name, id_column):
        column_query = """SELECT column_name FROM information_schema.columns WHERE table_name = %s;"""

        try:
            cursor = self.get_conn().cursor()

            cursor.execute(column_query, (table_name,))

            columns = cursor.fetchall()

            col_arr = []
            for column in columns:
                col_arr.append("%s=%s.%s" % (column[0], tmp_table_name, column[0]))
            cols = ", ".join(col_arr)

            update_query = """UPDATE %(table)s SET %(cols)s FROM %(tmp_table)s WHERE %(table)s.%(id)s=%(tmp_table)s.%(id)s;"""
            update_query = update_query % {'table': table_name, 'cols': cols, 'tmp_table': tmp_table_name, 'id': id_column}
            insert_query = """INSERT INTO %(table)s
                                SELECT %(tmp)s.*
                                FROM %(tmp)s
                                LEFT JOIN %(table)s ON %(tmp)s.%(id)s=%(table)s.%(id)s
                                WHERE %(table)s.%(id)s IS NULL;"""

            insert_query = insert_query % {'table': table_name, 'tmp': tmp_table_name, 'id': id_column}

            cursor.execute(update_query)
            cursor.execute(insert_query)

            cursor.connection.commit()
            cursor.connection.close()
        except Exception, e:
            print(e)
            raise

    def swap_and_drop(self, table1, table2):
        tmp_name = table1 + "_sdtmp"
        query = """ALTER TABLE %s RENAME TO %s;
            ALTER TABLE %s RENAME TO %s;
            DROP TABLE %s;"""

        cursor = self.get_conn().cursor()
        cursor.execute(query % (table1, tmp_name, table2, table1, tmp_name))
        cursor.connection.commit()
        cursor.connection.close()

    def drop_table(self, table_name):
        query = """DROP TABLE %s;"""
        cursor = self.get_conn().cursor()
        cursor.execute(query % (table_name))
        cursor.connection.commit()
        cursor.connection.close()

    def readonly_select(self, table_name):
        query = """GRANT SELECT ON %s TO readonly;""" % table_name
        cursor = self.get_conn().cursor()
        cursor.execute(query)
        cursor.connection.commit()
        cursor.connection.close()

    def vacuum_all(self):
        query = "VACUUM;"

        cursor = self.get_conn().cursor()
        cursor.execute(query)
        cursor.connection.close()


def get_s3_credentials():
    return "aws_access_key_id=%s;aws_secret_access_key=%s" % (config.get('S3.access_key'), config.get('S3.secret'))
