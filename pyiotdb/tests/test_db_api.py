import os
import unittest

from pyiotdb.db import connect


class DBAPITest(unittest.TestCase):

    def setUp(self) -> None:
        ip = os.environ.get('ip')
        port = os.environ.get('port', 6667)
        username = os.environ.get('username', 'root')
        password = os.environ.get('password', 'root')
        self.conn = connect(host=ip, port=port, username=username, password=password)

    def test_query(self):
        cursor = self.conn.cursor()
        cursor.execute('show  storage group')
        print(cursor.rowcount)

    def test_query_data(self):
        cursor = self.conn.cursor()
        cursor.execute('select * from  `root.unite02450.line_id.1705.machine_id.11`')
        for row in cursor.fetchall():
            print(row.Time)
