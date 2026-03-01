# CobaltDB v1.1

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

## Roadmap (v1.2+)

- [ ] Foreign keys
- [ ] More JOIN types (LEFT, RIGHT, OUTER)
- [ ] Subqueries
- [ ] Table constraints (UNIQUE, CHECK)
- [ ] More data types (DATE, TIMESTAMP, etc.)
- [ ] Performance optimizations

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
