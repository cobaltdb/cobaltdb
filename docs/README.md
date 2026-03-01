# CobaltDB Documentation

## Overview

CobaltDB is a lightweight, embeddable database engine written in Go with SQL and JSON query support, persistent storage, in-memory mode, and transaction support.

## Features

- **SQL Support**: Full SQL parser with SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE INDEX
- **WHERE Clause Filtering**: Complete WHERE clause support with comparison operators (=, !=, <, >, <=, >=, IS NULL, IS NOT NULL)
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

## CLI Usage

### Installation

```bash
go install github.com/cobaltdb/cobaltdb/cmd/cobaltdb-cli@latest
```

### Commands

```bash
# In-memory database
cobaltdb -memory "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

# Disk database
cobaltdb -path ./mydb.db "SELECT * FROM users"

# Interactive mode
cobaltdb -memory -i
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
│  - Schema management                 │
│  - Table operations                  │
│  - Data persistence (disk)           │
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
│  - Disk I/O                         │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│        BTree (pkg/btree)            │
│  - B+Tree implementation            │
│  - In-memory index                  │
└─────────────────────────────────────┘
```

## Data Types

- **INTEGER**: 64-bit signed integer
- **TEXT**: UTF-8 string
- **REAL**: 64-bit floating point
- **BOOLEAN**: Boolean (true/false)
- **JSON**: JSON text

## License

MIT License - see [LICENSE](../LICENSE) file.
