import os
import unittest
from sqlalchemy.engine.url import URL
from sqlalchemy.engine import create_engine
import sqlalchemy

from pyiotdb import utils


class TestSQLAlchemy(unittest.TestCase):

    def setUp(self):
        self.driver_name = 'iotdb'

        iot_uri = os.environ.get('db_uri')
        self.engine = create_engine(iot_uri)
        self.connection = self.engine.connect()

    def test_simple_query(self):

        rows = self.connection.execute("select * from  root.unite76072.line_id.1705.machine_id.11 ").fetchmany(10)

        print(rows)

    def test_time_query(self):
        sql = 'select zt11470  from  root.unite87764.line_id.1705.machine_id.11 where time  >2021-06-01T07:35:30.593+08:00 and time  < 2021-06-01T07:40:30.593+08:00'
        result = self.connection.execute(sql)
        for row in result.fetchmany(10):
            print(row)

    def test_get_schemas(self):
        insp = sqlalchemy.inspect(self.engine)

        schemas = insp.get_schema_names()

        for schema in schemas:
            print("schema: %s" % schema)
            for table_name in insp.get_table_names(schema=schema):
                # print('table: {}'.format(table_name))
                for column in insp.get_columns(table_name, schema=schema):
                    print("Column: %s" % column)

    def test_ddl_query(self):

        result = self.connection.execute("SHOW STORAGE GROUP")
        # df = utils.resultset_to_pandas(result)

        for row in result.fetchall():
            print(row['storage group'])

    def test_create_storage_group(self):
        query = 'create storage group root.lfc'
        result = self.connection.execute(query)
        for row in result.fetchmany(1):
            print(row)

    def test_select1(self):
        result = self.connection.execute("select * from  `root.unite02450.line_id.1705.machine_id.11`")
        s = result.rowcount()
        print(s)
