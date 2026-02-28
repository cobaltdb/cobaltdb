# CobaltDB v1.0

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
# Run demo
go run cmd/demo/main.go

# Run tests
go test ./...

# Run CLI
go run cmd/cobaltdb-cli/main.go

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
- WHERE clause with operators: =, !=, <, >, <=, >=, IS NULL, IS NOT NULL
- Placeholder support (?) for prepared statements
- In-memory and disk-based storage
- Data persistence (survives restart)
- Transactions (BEGIN, COMMIT, ROLLBACK)

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

## Roadmap (v1.1+)

- [ ] WAL (Write-Ahead Log) for crash recovery
- [ ] B+Tree disk persistence
- [ ] Index usage in query execution
- [ ] Query optimizer
- [ ] More SQL functions (COUNT, SUM, AVG, etc.)
- [ ] Foreign keys
- [ ] JOIN support

## License

MIT License - see [LICENSE](LICENSE) file.

---

Built with ❤️ by Ersin KOÇ
