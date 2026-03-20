# CobaltDB Python SDK

Connect to CobaltDB server using the MySQL protocol.

## Installation

```bash
pip install mysql-connector-python
# or
pip install PyMySQL
```

Then copy `cobaltdb.py` to your project, or install from source:

```bash
pip install -e sdk/python/
```

## Usage

```python
import cobaltdb

# Connect to CobaltDB server
conn = cobaltdb.connect(host='127.0.0.1', port=3307, user='admin')

# Create table
conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

# Insert data
conn.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
conn.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')")

# Query data
cursor = conn.execute("SELECT * FROM users ORDER BY id")
for row in cursor.fetchall():
    print(row)

# JSON support
conn.execute("CREATE TABLE docs (id INTEGER PRIMARY KEY, data JSON)")
conn.execute("INSERT INTO docs VALUES (1, '{\"name\": \"test\", \"tags\": [\"a\", \"b\"]}')")

cursor = conn.execute("SELECT JSON_EXTRACT(data, '$.name') FROM docs WHERE id = 1")
print(cursor.fetchone())

conn.close()
```

## With Context Manager

```python
with cobaltdb.connect(host='127.0.0.1', port=3307) as conn:
    conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO test VALUES (1)")
    cursor = conn.execute("SELECT * FROM test")
    print(cursor.fetchall())
```

## Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `host` | `127.0.0.1` | Server hostname |
| `port` | `3307` | MySQL protocol port |
| `user` | `admin` | Username |
| `password` | `""` | Password |
| `database` | `""` | Database name |
| `autocommit` | `True` | Auto-commit mode |
