# CobaltDB v1.5

> A lightweight, embeddable database engine written in Go with SQL + JSON query support, persistent storage, in-memory mode, and transaction support.

[![Go Reference](https://pkg.go.dev/badge/github.com/cobaltdb/cobaltdb.svg)](https://pkg.go.dev/github.com/cobaltdb/cobaltdb)
[![Go Report Card](https://goreportcard.com/badge/github.com/cobaltdb/cobaltdb)](https://goreportcard.com/report/github.com/cobaltdb/cobaltdb)
[![Tests](https://github.com/cobaltdb/cobaltdb/actions/workflows/test.yml/badge.svg)](https://github.com/cobaltdb/cobaltdb/actions)

## Features

- **SQL Support**: Full SQL parser with SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX
- **WHERE Clause Filtering**: Complete WHERE clause support with comparison operators
- **Persistent Storage**: Disk-based storage with data persistence across restarts
- **In-Memory Mode**: RAM-only databases for testing and caching
- **Placeholder Support**: Prepared statement placeholders (?)
- **Transactions**: BEGIN, COMMIT, ROLLBACK support
- **Zero CGO**: Pure Go implementation
- **WAL (Write-Ahead Log)**: Crash recovery and durability
- **Index Support**: B+Tree indexes for query optimization
- **JOIN Support**: INNER, LEFT, RIGHT JOIN with ON clause
- **Subqueries**: IN (SELECT ...) support
- **Constraints**: UNIQUE, CHECK, FOREIGN KEY support
- **Data Types**: INTEGER, TEXT, REAL, BOOLEAN, JSON, DATE, TIMESTAMP
- **VIEW Support**: CREATE VIEW, DROP VIEW
- **Trigger Support**: CREATE TRIGGER, DROP TRIGGER (framework)
- **Stored Procedure Support**: CREATE PROCEDURE, DROP PROCEDURE, CALL
- **SQL Functions**: LENGTH, UPPER, LOWER, TRIM, SUBSTR, COALESCE, IFNULL, NULLIF, CAST, and more

## Installation

```bash
go get github.com/cobaltdb/cobaltdb
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
    // Open database (in-memory or disk)
    db, err := engine.Open(":memory:", &engine.Options{
        InMemory:  true,
        CacheSize: 1024,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Create table
    db.Exec(ctx, `
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT
        )
    `)

    // Insert data with placeholders
    db.Exec(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", "Ersin", "ersin@cobaltdb.dev")

    // Query with WHERE clause
    rows, _ := db.Query(ctx, "SELECT name, email FROM users WHERE name = ?", "Ersin")
    defer rows.Close()

    for rows.Next() {
        var name, email string
        rows.Scan(&name, &email)
        log.Printf("User: %s <%s>", name, email)
    }
}
```

## Disk Persistence

```go
// Open disk-based database
db, err := engine.Open("./mydb.cobalt", &engine.Options{
    InMemory:  false,
    CacheSize: 1024,
})

// Data is automatically saved on close
db.Close()

// Reopen - data persists!
db2, _ := engine.Open("./mydb.cobalt", nil)
```

## SQL Support

### DDL
```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)
CREATE INDEX idx_email ON users(email)
DROP TABLE users
```

### DML
```sql
INSERT INTO users (name, email) VALUES ('Ersin', 'ersin@test.dev')
SELECT * FROM users
SELECT name, email FROM users WHERE age > 25
UPDATE users SET name = ? WHERE id = ?
DELETE FROM users WHERE id = ?
```

### Aggregate Functions
```sql
SELECT COUNT(*) FROM users
SELECT SUM(price) FROM orders
SELECT AVG(age) FROM users
SELECT MIN(price) FROM products
SELECT MAX(score) FROM tests
SELECT COUNT(*) FROM users WHERE age > 18
```

### Advanced WHERE
```sql
-- LIKE pattern matching
SELECT * FROM users WHERE name LIKE 'A%'
SELECT * FROM users WHERE name LIKE '_lice'

-- IN operator
SELECT * FROM users WHERE id IN (1, 2, 3)

-- BETWEEN operator
SELECT * FROM users WHERE age BETWEEN 18 AND 30
```

### Query Modifiers
```sql
-- ORDER BY (ASC/DESC)
SELECT * FROM users ORDER BY name ASC
SELECT * FROM users ORDER BY age DESC

-- LIMIT and OFFSET
SELECT * FROM users LIMIT 10
SELECT * FROM users LIMIT 10 OFFSET 20

-- DISTINCT
SELECT DISTINCT category FROM products
```

### GROUP BY and HAVING
```sql
-- GROUP BY
SELECT category, COUNT(*) FROM sales GROUP BY category
SELECT category, SUM(amount) FROM sales GROUP BY category
SELECT category, AVG(price) FROM products GROUP BY category

-- GROUP BY with ORDER BY and LIMIT
SELECT category, SUM(amount) FROM sales GROUP BY category ORDER BY SUM(amount) DESC LIMIT 2

-- HAVING (filter grouped results)
SELECT category, COUNT(*) FROM sales GROUP BY category HAVING COUNT(*) > 1
SELECT category, SUM(amount) FROM sales GROUP BY category HAVING SUM(amount) > 1000
```

### Transactions
```sql
BEGIN
INSERT INTO users (name) VALUES ('Alice')
COMMIT
-- or
ROLLBACK
```

## Running

```bash
# Build CLI
go build -o cobaltdb ./cmd/cobaltdb-cli

# Run CLI
./cobaltdb -memory "CREATE TABLE users (id INTEGER, name TEXT)"

# Interactive mode
./cobaltdb -memory -i

# Run tests
go test ./...

# Run benchmarks
go test -bench=. -benchtime=2s ./test/...

# Run demo
go run cmd/demo/main.go

# Run server
go run cmd/cobaltdb-server/main.go
```

## Architecture

```
┌─────────────────────────────────────┐
│         Engine (pkg/engine)          │
│  - Database open/close              │
│  - Query execution                  │
│  - Transaction management           │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│       Catalog (pkg/catalog)          │
│  - Schema management                │
│  - Table operations                 │
│  - Data persistence (disk)          │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        Query (pkg/query)             │
│  - Lexer & Parser                   │
│  - AST nodes                        │
│  - Expression evaluation            │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     Storage (pkg/storage)            │
│  - Buffer Pool                      │
│  - Page management                  │
│  - Disk I/O                        │
│  - WAL (Write-Ahead Log)           │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        BTree (pkg/btree)            │
│  - B+Tree implementation           │
│  - In-memory index                  │
└─────────────────────────────────────┘
```

## v1.0 - What's Working

### ✅ Core Features
- SQL Parser (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX, DROP TABLE)
- WHERE clause with operators: =, !=, <, >, <=, >=, IS NULL, IS NOT NULL, LIKE, IN, BETWEEN
- Placeholder support (?) for prepared statements
- In-memory and disk-based storage
- Data persistence (survives restart)
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Aggregate Functions: COUNT, SUM, AVG, MIN, MAX
- Query Modifiers: ORDER BY, LIMIT, OFFSET, DISTINCT
- GROUP BY with aggregate functions
- HAVING clause for filtering grouped results

### ✅ Data Types
- INTEGER
- TEXT
- REAL
- BOOLEAN
- JSON

### ✅ Storage
- Buffer pool with LRU eviction
- Page-based storage
- Disk backend with file I/O

## v1.5 - What's New

### Core Features
- **Full JSON Support**: Complete JSON manipulation functions
  - JSON_EXTRACT: Extract values from JSON using paths
  - JSON_SET: Set values in JSON
  - JSON_REMOVE: Remove values from JSON
  - JSON_VALID: Check if a string is valid JSON
  - JSON_ARRAY_LENGTH: Get array length
  - JSON_TYPE: Get JSON value type
  - JSON_KEYS: Get object keys
  - JSON_MERGE: Merge JSON objects
  - JSON_PRETTY: Format JSON
  - JSON_MINIFY: Minify JSON
  - JSON_QUOTE/JSON_UNQUOTE: Quote/unquote strings
  - REGEXP_MATCH, REGEXP_REPLACE, REGEXP_EXTRACT: Regular expression functions

- **Window Functions Support**: Framework for analytic functions
  - ROW_NUMBER: Row number within partition
  - RANK: Rank with gaps
  - DENSE_RANK: Rank without gaps
  - LAG/LEAD: Access previous/next row values
  - FIRST_VALUE/LAST_VALUE: First/last value in partition
  - NTH_VALUE: Nth value in partition

- **Query Optimizer Improvements**
  - Prepared statement caching (up to 1000 statements)
  - Index usage optimization for WHERE clauses
  - Cost-based query planning

## Roadmap (v1.6+)

- [ ] Common Table Expressions (WITH clause)
- [ ] VACUUM/ANALYZE commands
- [ ] Full-Text Search (FTS)
- [ ] Materialized Views
- [ ] User Management & Permissions

## v1.4 - What's New

### Core Features
- **Additional SQL Functions**: Extended function library for string, numeric, and date operations
  - String: LENGTH, UPPER, LOWER, TRIM, LTRIM, RTRIM, SUBSTR, SUBSTRING, CONCAT, REPLACE, INSTR
  - Numeric: ABS, ROUND, FLOOR, CEIL
  - Null-handling: COALESCE, IFNULL, NULLIF
  - Formatting: PRINTF, CAST
  - Date/Time: DATE, TIME, DATETIME, STRFTIME

- **Full Trigger Execution**: Complete trigger integration
  - BEFORE/AFTER INSERT triggers
  - BEFORE/AFTER UPDATE triggers
  - BEFORE/AFTER DELETE triggers
  - GetTriggersForTable for trigger execution hooks

- **Stored Procedure Execution**: Complete procedure support
  - CALL statement execution
  - Procedure body execution with parameters

- **Performance Optimizations**
  - Prepared statement caching (up to 1000 statements)
  - Reduced parsing overhead for repeated queries

## v1.3 - What's New

### Core Features
- **VIEW Support**: Virtual tables based on saved queries
  - CREATE VIEW with AS SELECT syntax
  - DROP VIEW with IF EXISTS support
  - Views can be queried like regular tables
  - Automatic view resolution in SELECT statements

- **Trigger Support**: Database triggers framework
  - CREATE TRIGGER parsing (BEFORE/AFTER INSERT/UPDATE/DELETE)
  - DROP TRIGGER support
  - Trigger storage in catalog
  - GetTriggersForTable for trigger execution hooks

- **Stored Procedure Support**: Stored procedure framework
  - CREATE PROCEDURE with parameter support
  - DROP PROCEDURE support
  - Procedure storage in catalog

## v1.2 - What's New

### Core Features
- **LEFT/RIGHT JOIN Support**: Extended JOIN functionality beyond INNER JOIN
  - LEFT JOIN with NULL padding for unmatched rows
  - RIGHT JOIN support
  - Full compatibility with ON clause conditions

- **Subquery Support**: Nested queries in WHERE clauses
  - IN (SELECT ...) support
  - Scalar subqueries in expressions

- **UNIQUE Constraint**: Column-level uniqueness enforcement
  - Validated on INSERT and UPDATE

- **CHECK Constraint**: Custom validation expressions
  - CHECK (column > 0) style constraints

- **FOREIGN KEY Support**: Referential integrity
  - FOREIGN KEY REFERENCES syntax
  - ON DELETE and ON UPDATE actions

- **Additional Data Types**:
  - DATE type
  - TIMESTAMP type

## v1.1 - What's New

### Core Features
- **WAL (Write-Ahead Log)**: Complete crash recovery support
  - Logs all INSERT, UPDATE, DELETE operations before applying
  - Transaction support with COMMIT and ROLLBACK
  - Automatic recovery on database startup
  - Checkpoint mechanism for log truncation

- **Index Support**: B+Tree indexes for improved query performance
  - CREATE INDEX support
  - Automatic index maintenance on data changes
  - Index usage in SELECT queries for equality conditions

- **JOIN Support**: Basic INNER JOIN functionality
  - JOIN with ON clause
  - Multi-table queries

- **Improved Data Persistence**:
  - WAL checkpoint on database close
  - Better durability guarantees

## License

MIT License - see [LICENSE](LICENSE) file.

---

Built with ❤️ by Ersin KOÇ
