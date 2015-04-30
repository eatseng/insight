from __future__ import absolute_import

import oursql
from clay import config


class Mysql(object):

    def __init__(self):
        self.conn_opts = dict(
            host=config.get("mysql_db.host"),
            port=config.get("mysql_db.port"),
            user=config.get("mysql_db.user"),
            passwd=config.get("mysql_db.password"),
            db=config.get("mysql_db.db")
        )

    def get_conn(self, dict_cursor=False):
        return oursql.connect(**self.conn_opts)

    def execute(self, select_sql, data=[]):
        conn = self.get_conn(False)
        cursor = conn.cursor()
        cursor.execute(select_sql, data)
        cursor.connection.commit()
        conn.close()

    def select(self, select_sql, dict_cursor=False):
        conn = self.get_conn(dict_cursor)
        cursor = conn.cursor()
        cursor.execute(select_sql)
        result = cursor.fetchall()
        conn.close()
        if result is None:
            return None
        else:
            return result

    def drop_table(self, table):
        drop_table_query = """DROP TABLE IF EXISTS %s;""" % table
        self.execute(drop_table_query)

    def if_table_exists(self, table):
        query = """SELECT count(*) FROM information_schema.tables WHERE (table_name = '%s');""" % table

        if self.select(query, False)[0][0] == 1:
            return True
        else:
            return False
