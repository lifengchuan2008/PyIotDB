from pyiotdb.db import connect
from pyiotdb.exceptions import (
    DatabaseError,
    DataError,
    Error,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    Warning,
)

__all__ = [
    "connect",
    "apilevel",
    "threadsafety",
    "paramstyle",
    "DataError",
    "DatabaseError",
    "Error",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "Warning",
]

apilevel = "2.0"
# Threads may share the module and connections
threadsafety = 2
paramstyle = "pyformat"
