from __future__ import absolute_import

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from clay import config

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


class Redshift(object):

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
