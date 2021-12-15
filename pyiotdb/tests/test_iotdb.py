import itertools
import os
import unittest
from iotdb.Session import Session

from pyiotdb import utils


class IotdbTest(unittest.TestCase):

    def setUp(self):
        ip = os.environ.get('ip')
        port = os.environ.get('port', 6667)
        username = os.environ.get('username', 'root')
        password = os.environ.get('password', 'root')
        session = Session(ip, port, username, password)
        session.open(False)
        self.session = session

    def test_query(self):

        result = self.session.execute_query_statement("show storage group")
        df = utils.resultset_to_pandas(result)
        print(df.columns)

        for row in df.iterrows():
            # print(row)
            print(row[1]['storage group'])
            # print(df.items[1])

            # while (result.has_next()):
            #     record = result.next()
            #     fields = record.get_fields()
            #     for f in fields:
            #         print(f)

            # print(df.values)

            # result = session.execute_query_statement("show devices root.unite87764")

            # while (result.has_next()):
            #     print(result.next())
            #
            # result = session.execute_query_statement("select * from  root.unite87764.line_id.1705.machine_id.11")
            #
            # print(result.get_fetch_size())
            #
            # df = result.todf()
            #
            # for row in df.itertuples():
            #     print(type(df.itertuples()))

            # columnTypes = result.get_column_types()
            #
            # names = result.get_column_names()
            # for i in range(0, len(names)):
            #     print(names[i])
            #     print(columnTypes[i])
            #
            # # for name in names:
            # #     if name == 'Time':
            # #         continue
            # #     if name.index('.') > 0:
            # #         name = name.split('.')[-1]
            # #         print('name: {}'.format(name))
            #
            # # for col in columnTypes:
            # #     print(col.value)
            # #     print(col)
            # #
            # if result.has_next():
            #     record = result.next()
            #     print(record)

    def test_query_devices(self):

        result = self.session.execute_query_statement("SHOW DEVICES root.unite76072")
        df = utils.resultset_to_pandas(result)
        print(df.columns)

        for row in df.iterrows():
            # print(row)
            print(row[1]['devices'])
        # print(df.items[1])

    # TIMESERIES
    def test_query_timeseries(self):

        result = self.session.execute_query_statement("SHOW TIMESERIES root.unite76072.line_id.1705.machine_id.11")
        df = utils.resultset_to_pandas(result)
        print(df.columns)

        for row in df.iterrows():
            # print(row)
            timeseries = row[1]['timeseries']
            dataType = row[1]['dataType']
            print(timeseries.split('.')[-1])
            print(dataType)
        # print(df.items[1])

    def test_query_records(self):

        result = self.session.execute_query_statement("select * from  root.unite76072.line_id.1705.machine_id.11")
        df = utils.resultset_to_pandas(result)
        # df = df[:10]
        d = list(df.to_records(index=False))

        print(d[:2])

        # columnTypes = result.get_column_types()
        #
        # names = result.get_column_names()
        # for i in range(0, len(names)):
        #     name = names[i]
        #     if name.find('.') > 0:
        #         name = name.split('.')[-1]
        #
        #     dataType = columnTypes[i]
        #     print(dataType.name)

    def test_ddl_query(self):

        result = self.session.execute_query_statement("SHOW STORAGE GROUP")
        df = utils.resultset_to_pandas(result)
        print(df)

    def test_create_storage_group(self):
        query = 'create storage group root.lfc'
        result = self.session.execute_query_statement(query)
        print(result)
