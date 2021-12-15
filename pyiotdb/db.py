import sys

from iotdb.Session import Session

from pyiotdb import constants, common, exceptions, utils
from pyiotdb.common import DBAPITypeObject


def connect(
        host="localhost",
        port=6667,
        username=None,
        password=None,
):
    return Connection(
        host,
        port,
        username,
        password)


class Connection(object):

    def __init__(self,
                 host=None,
                 port=None,
                 username=None,
                 password=None,
                 ):
        if port is None:
            port = 6667
        self._session = Session(host, port, username, password)
        self._session.open(False)

    def close(self):
        """Close the underlying session and Thrift transport"""
        self._session.close()

    def commit(self):
        pass

    def rollback(self):
        raise exceptions.NotSupportedError("iotdb does not have transactions")

    @property
    def session(self):
        return self._session

    def cursor(self, *args, **kwargs):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(self, *args, **kwargs)


# https://www.python.org/dev/peps/pep-0249/
class Cursor(common.DBAPICursor):

    def __init__(self, connection, arraysize=10000):
        self._results = None
        self._arraysize = arraysize
        self._connection = connection
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # def _reset_state(self):
    #     self.description = None

    @property
    def arraysize(self):
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value):
        """Array size cannot be None, and should be an integer"""
        default_arraysize = 10000
        try:
            self._arraysize = int(value) or default_arraysize
        except TypeError:
            self._arraysize = default_arraysize

    def close(self):
        self._reset_state()

    def __next__(self):
        try:
            yield next(self._results)
        except StopIteration:
            return

    next = __next__

    @property
    def rowcount(self):
        results = list(self._results)
        n = len(results)
        self._results = iter(results)
        return n

    @property
    def lastrowid(self):
        pass

    @property
    def rownumber(self):
        results = list(self._results)
        n = len(results)
        self._results = iter(results)
        return n

    def fetchone(self):
        return self.next()

    def fetchmany(self, size=None):
        size = size or self.arraysize
        return list(self._results)[:size]

    def fetchall(self):
        return self._results

    def execute(self, operation, parameters=None):
        # Prepare statement
        if parameters is None:
            sql = operation
        else:
            sql = operation % _escaper.escape_args(parameters)

        if sql == 'SELECT 1':
            sql = 'SHOW VERSION'
        if sql.find('`') > 0:
            sql = sql.replace('`', '')

        result = self._connection.session.execute_query_statement(sql)

        # update description
        self.description = get_description_from_row(result)
        df = utils.resultset_to_pandas(result)
        self._results = df.itertuples(index=False)


class IotDBParamEscaper(common.ParamEscaper):
    def escape_string(self, item):
        # backslashes and single quotes need to be escaped
        # TODO verify against parser
        # Need to decode UTF-8 because of old sqlalchemy.
        # Newer SQLAlchemy checks dialect.supports_unicode_binds before encoding Unicode strings
        # as byte strings. The old version always encodes Unicode as byte strings, which breaks
        # string formatting here.
        if isinstance(item, bytes):
            item = item.decode('utf-8')
        return "'{}'".format(
            item
                .replace('\\', '\\\\')
                .replace("'", "\\'")
                .replace('\r', '\\r')
                .replace('\n', '\\n')
                .replace('\t', '\\t')
        )


_escaper = IotDBParamEscaper()

for type in constants.TYPES:
    name = constants.TYPE_NAMES[type]
    setattr(sys.modules[__name__], name, DBAPITypeObject([name]))


def get_description_from_row(result):
    description = []

    columnTypes = result.get_column_types()

    names = result.get_column_names()
    for i in range(0, len(names)):
        name = names[i]
        if name.find('.') > 0:
            name = name.split('.')[-1]

        dataType = columnTypes[i]

        type_code = constants.TYPE_MAP[dataType.name]

        description.append(
            (
                name,  # name
                type_code,  # type_code
                None,  # [display_size]
                None,  # [internal_size]
                None,  # [precision]
                None,  # [scale]
                name == 'Time',  # [null_ok]
            )
        )

    return description
