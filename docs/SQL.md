# SQL Reference

## Data Definition Language (DDL)

### CREATE TABLE

```sql
CREATE TABLE table_name (
    column_name datatype [constraints],
    ...
);
```

**Example:**
```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    age INTEGER DEFAULT 18
);
```

**Supported Data Types:**
- `INTEGER` - 64-bit signed integer
- `TEXT` - UTF-8 string
- `REAL` - 64-bit floating point
- `BOOLEAN` - Boolean value
- `JSON` - JSON text

**Constraints:**
- `PRIMARY KEY` - Primary key (auto-increment if not specified)
- `NOT NULL` - Column cannot be NULL

### CREATE INDEX

```sql
CREATE INDEX index_name ON table_name(column_name);
```

**Example:**
```sql
CREATE INDEX idx_email ON users(email);
```

### DROP TABLE

```sql
DROP TABLE table_name;
```

**Example:**
```sql
DROP TABLE users;
```

## Data Manipulation Language (DML)

### INSERT

```sql
INSERT INTO table_name (columns...) VALUES (values...);
```

**Example:**
```sql
-- Single row
INSERT INTO users (name, email) VALUES ('John', 'john@example.com');

-- Multiple rows
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');

-- With placeholders
INSERT INTO users (name, email) VALUES (?, ?);
```

### SELECT

```sql
SELECT columns... FROM table_name [WHERE condition] [ORDER BY column];
```

**Example:**
```sql
-- All columns
SELECT * FROM users;

-- Specific columns
SELECT name, email FROM users;

-- With WHERE
SELECT * FROM users WHERE age > 18;

-- With multiple conditions
SELECT * FROM users WHERE age >= 18 AND email IS NOT NULL;

-- Ordered results
SELECT * FROM users ORDER BY name;
```

**Supported Operators:**
- `=` - Equal
- `!=` - Not equal
- `<` - Less than
- `>` - Greater than
- `<=` - Less than or equal
- `>=` - Greater than or equal
- `IS NULL` - Is NULL
- `IS NOT NULL` - Is not NULL
- `AND` - Logical AND
- `OR` - Logical OR

### UPDATE

```sql
UPDATE table_name SET column = value [, ...] [WHERE condition];
```

**Example:**
```sql
-- Update single row
UPDATE users SET age = 25 WHERE name = 'John';

-- Update multiple rows
UPDATE users SET status = 'active' WHERE age >= 18;

-- With placeholders
UPDATE users SET age = ? WHERE id = ?;
```

### DELETE

```sql
DELETE FROM table_name [WHERE condition];
```

**Example:**
```sql
-- Delete specific row
DELETE FROM users WHERE id = 1;

-- Delete multiple rows
DELETE FROM users WHERE age < 18;

-- Delete all rows
DELETE FROM users;
```

## Transactions

```sql
BEGIN;
-- SQL statements
COMMIT;
```

Or with rollback:
```sql
BEGIN;
INSERT INTO users (name) VALUES ('Alice');
ROLLBACK;
```

## Placeholders

Use `?` for parameterized queries:

```go
db.Exec(ctx, "INSERT INTO users (name, age) VALUES (?, ?)", "John", 30)
db.Query(ctx, "SELECT * FROM users WHERE age > ?", 18)
```

## JSON Support

CobaltDB supports JSON data type:

```sql
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT,
    attributes JSON
);

INSERT INTO products (name, attributes) VALUES (
    'Laptop',
    '{"color": "silver", "ram": "16GB"}'
);
```

Query JSON data using LIKE:

```sql
SELECT * FROM products WHERE attributes LIKE '%"color"%';
```
