"""
CobaltDB Python SDK

Connect to CobaltDB server using the MySQL protocol.
Uses mysql-connector-python or PyMySQL under the hood.

Usage:
    import cobaltdb

    # Connect to CobaltDB server
    conn = cobaltdb.connect(host='127.0.0.1', port=3307, user='admin')

    # Execute queries
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO users VALUES (1, 'Alice')")

    # Query data
    cursor.execute("SELECT * FROM users")
    for row in cursor.fetchall():
        print(row)

    conn.close()

Requirements:
    pip install mysql-connector-python
    # or
    pip install PyMySQL
"""

__version__ = "0.3.0"
__all__ = ["connect", "Connection", "Cursor", "Error", "CobaltDBError"]


class CobaltDBError(Exception):
    """Base exception for CobaltDB SDK errors."""
    pass


class Error(CobaltDBError):
    """Database error."""
    pass


def connect(host="127.0.0.1", port=3307, user="admin", password="",
            database="", autocommit=True, **kwargs):
    """
    Connect to a CobaltDB server.

    CobaltDB speaks the MySQL wire protocol, so this function
    creates a standard MySQL connection under the hood.

    Args:
        host: Server hostname (default: 127.0.0.1)
        port: MySQL protocol port (default: 3307)
        user: Username (default: admin)
        password: Password (default: empty)
        database: Database name (default: empty)
        autocommit: Auto-commit mode (default: True)
        **kwargs: Additional arguments passed to the MySQL connector

    Returns:
        Connection object

    Example:
        conn = cobaltdb.connect(host='localhost', port=3307, user='admin')
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        print(cursor.fetchone())
    """
    try:
        import mysql.connector
        conn = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=autocommit,
            **kwargs
        )
        return Connection(conn, _driver="mysql-connector")
    except ImportError:
        pass

    try:
        import pymysql
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            autocommit=autocommit,
            **kwargs
        )
        return Connection(conn, _driver="pymysql")
    except ImportError:
        pass

    raise CobaltDBError(
        "No MySQL driver found. Install one of:\n"
        "  pip install mysql-connector-python\n"
        "  pip install PyMySQL"
    )


class Connection:
    """CobaltDB connection wrapper."""

    def __init__(self, conn, _driver="unknown"):
        self._conn = conn
        self._driver = _driver

    def cursor(self, **kwargs):
        """Create a new cursor."""
        return Cursor(self._conn.cursor(**kwargs))

    def commit(self):
        """Commit the current transaction."""
        self._conn.commit()

    def rollback(self):
        """Rollback the current transaction."""
        self._conn.rollback()

    def close(self):
        """Close the connection."""
        self._conn.close()

    def execute(self, sql, params=None):
        """Shorthand: execute a query and return the cursor."""
        cursor = self.cursor()
        cursor.execute(sql, params)
        return cursor

    @property
    def driver_name(self):
        """Return the underlying MySQL driver name."""
        return self._driver

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class Cursor:
    """CobaltDB cursor wrapper."""

    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, sql, params=None):
        """Execute a SQL statement."""
        if params:
            self._cursor.execute(sql, params)
        else:
            self._cursor.execute(sql)
        return self

    def executemany(self, sql, params_list):
        """Execute a SQL statement with multiple parameter sets."""
        self._cursor.executemany(sql, params_list)
        return self

    def fetchone(self):
        """Fetch one row."""
        return self._cursor.fetchone()

    def fetchall(self):
        """Fetch all rows."""
        return self._cursor.fetchall()

    def fetchmany(self, size=None):
        """Fetch many rows."""
        if size:
            return self._cursor.fetchmany(size)
        return self._cursor.fetchmany()

    @property
    def description(self):
        """Column descriptions."""
        return self._cursor.description

    @property
    def rowcount(self):
        """Number of affected rows."""
        return self._cursor.rowcount

    @property
    def lastrowid(self):
        """Last inserted row ID."""
        return self._cursor.lastrowid

    def close(self):
        """Close the cursor."""
        self._cursor.close()

    def __iter__(self):
        return iter(self._cursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
