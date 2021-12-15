from sqlalchemy import types
# TODO shouldn't use mysql type
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy.sql.compiler import SQLCompiler
from sqlalchemy.sql.sqltypes import String

import pyiotdb

type_map = {
    "FLOAT": types.FLOAT,
    "DOUBLE": types.FLOAT,
    "TEXT": types.String,
    "INT32": types.BigInteger,
    "INT64": types.BigInteger,
    "BOOLEAN": types.BOOLEAN,
}


class IoTDBCompiler(SQLCompiler):
    pass


class UniversalSet(object):
    """set containing everything"""

    def __contains__(self, item):
        return True


class IoTDBIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = UniversalSet()


class IoTDBTypeCompiler(compiler.GenericTypeCompiler):

    def visit_REAL(self, type_, **kwargs):
        return "DOUBLE"

    def visit_NUMERIC(self, type_, **kwargs):
        return "LONG"

    visit_DECIMAL = visit_NUMERIC
    visit_INTEGER = visit_NUMERIC
    visit_SMALLINT = visit_NUMERIC
    visit_BIGINT = visit_NUMERIC
    visit_BOOLEAN = visit_NUMERIC
    visit_TIMESTAMP = visit_NUMERIC
    visit_DATE = visit_NUMERIC

    def visit_CHAR(self, type_, **kwargs):
        return "STRING"

    visit_NCHAR = visit_CHAR
    visit_VARCHAR = visit_CHAR
    visit_NVARCHAR = visit_CHAR
    visit_TEXT = visit_CHAR

    def visit_DATETIME(self, type_, **kwargs):
        return "LONG"

    def visit_TIME(self, type_, **kwargs):
        return "LONG"

    def visit_unicode(self, type_, **kw):
        return "STRING"


class IoTDBDialect(default.DefaultDialect):
    name = 'iotdb'
    driver = 'iotdb'
    paramstyle = 'pyformat'
    user = None
    password = None
    preparer = IoTDBIdentifierPreparer
    statement_compiler = IoTDBCompiler
    type_compiler = IoTDBTypeCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False

    # all str types must be converted in Unicode
    convert_unicode = True

    # Indicate whether the DB-API can receive SQL statements as Python
    #  unicode strings
    supports_unicode_statements = True
    supports_unicode_binds = True
    description_encoding = None

    if hasattr(String, "RETURNS_UNICODE"):
        returns_unicode_strings = String.RETURNS_UNICODE

    else:
        def _check_unicode_returns(self, connection, additional_tests=None):
            return True

        _check_unicode_returns = _check_unicode_returns

    @classmethod
    def dbapi(cls):
        return pyiotdb

    def create_connect_args(self, url):
        kwargs = {
            'host': url.host,
            'port': url.port or 6667,
            'username': url.username,
            'password': url.password,
        }
        kwargs.update(url.query)
        return [], kwargs

    def get_schema_names(self, connection, **kwargs):
        result = connection.execute("SHOW STORAGE GROUP")

        schemas = [row[0] for row in result.fetchall()]

        return schemas

    def get_table_names(self, connection, schema=None, **kwargs):
        query = "SHOW DEVICES  {schema}".format(
            schema=schema
        )
        result = connection.execute(
            query
        )
        tables = []
        for row in result.fetchall():
            device = row[0]

            device = str(device).replace(schema + '.', '')

            tables.append(device)

        return tables

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def get_table_options(self, connection, table_name, schema=None, **kwargs):
        return {}

    def has_table(self, connection, table_name, schema=None, **kw):
        result = self.connection.execute("show  devices {schema}.{table_name}".format(
            schema=schema,
            table_name=table_name
        ))
        s = result.fetchone()
        return s.devices

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        query = "SHOW TIMESERIES  {schema}.{table_name}".format(
            schema=schema,
            table_name=table_name
        )
        result = connection.execute(
            query
        )

        columns = []
        for row in result.fetchall():
            timeseries = row['timeseries']
            column = timeseries.split('.')[-1]
            dataType = row['dataType']
            columns.append({
                "name": column,
                "type": type_map[dataType],
                "nullable": True,
                "default": None,
            })
        return columns

    def get_keys(self):
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_foreign_keys(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_check_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kwargs):
        return {"text": ""}

    def get_indexes(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kwargs):
        return []

    def get_view_definition(self, connection, view_name, schema=None, **kwargs):
        pass

    def do_rollback(self, dbapi_connection):
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # We decode everything as UTF-8
        return True

    def _check_unicode_description(self, connection):
        # We decode everything as UTF-8
        return True
