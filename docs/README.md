# CobaltDB Documentation

## Overview

CobaltDB is a lightweight, embeddable database engine written in Go with SQL and JSON query support, persistent storage, in-memory mode, and transaction support.

## Features

- **SQL Support**: Full SQL parser with SELECT, INSERT, UPDATE, DELETE, DDL, CTEs, window functions, and JOINs
- **JSON & Full-Text Search**: Native JSON columns and text search capabilities
- **Vector Search**: HNSW-based vector similarity search
- **Persistent Storage**: Disk-based storage with WAL durability and page-level compression (zlib, LZ4, zstd)
- **In-Memory Mode**: RAM-only databases for testing and caching
- **Transactions**: ACID transactions with MVCC, deadlock detection, and savepoints
- **Placeholder Support**: Prepared statement placeholders (?)
- **Encryption at Rest**: AES-256-GCM transparent page encryption
- **Row-Level Security**: Policy-based access control
- **Replication**: Master-slave replication (async, sync, full_sync modes)
- **Backup & Restore**: Full, incremental, and differential backups
- **Query Optimization**: Cost-based optimizer with join reordering and index recommendations
- **Parallel Execution**: Multi-core parallel query processing
- **Foreign Data Wrappers**: Query external data sources via SQL
- **WebAssembly Runtime**: Compile and execute SQL via WASM
- **Zero CGO**: Pure Go implementation

## Operations

- [Operations Runbook](OPERATIONS_RUNBOOK.md): release gates, backup drills,
  recovery drills, monitoring checks, and incident playbooks.
- [Benchmarks](BENCHMARKS.md): bounded benchmark regression gate and historical
  performance notes.

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

### Quick Commands

```bash
# In-memory database
cobaltdb -memory "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"

# Disk database
cobaltdb -path ./mydb.db "SELECT * FROM users"

# Interactive mode (default when no SQL given)
cobaltdb -memory
```

### Subcommands

```bash
# Backups
cobaltdb -path ./mydb.db backup create [full|incremental|differential]
cobaltdb -path ./mydb.db backup list
cobaltdb -path ./mydb.db backup restore <id>
cobaltdb -path ./mydb.db backup delete <id>

# Maintenance
cobaltdb -path ./mydb.db vacuum
cobaltdb -path ./mydb.db analyze

# Import / Export
cobaltdb -path ./mydb.db import <file.csv> <table>
cobaltdb -path ./mydb.db export <table> <file.csv> [--format csv|json]

# Dump / Restore
cobaltdb -path ./mydb.db dump [file.sql]
cobaltdb -path ./mydb.db restore <file.sql>

# Observability
cobaltdb -path ./mydb.db metrics
cobaltdb -path ./mydb.db status
```

### Interactive Mode

Launch without SQL arguments to enter the interactive shell:

```bash
cobaltdb -path ./mydb.db
```

**Interactive features:**
- **Line editing** with arrow keys and persistent history (`~/.cobaltdb_history`)
- **Tab completion** for SQL keywords, meta-commands, and table names
- **Multi-line SQL** support (statements end with `;`)

**Meta-commands:**

| Command | Description |
|---|---|
| `.tables` | List all tables |
| `.schema [table]` | Show CREATE TABLE statement(s) |
| `.mode table\|csv\|json\|line` | Switch query output format |
| `.timer on\|off` | Toggle query execution timing |
| `.headers on\|off` | Toggle header row for table/csv output |
| `.dump [file.sql]` | Export database as SQL dump |
| `.restore <file.sql>` | Restore database from SQL dump |
| `.import <csv> <table>` | Import CSV into table |
| `.export <table> <csv>` | Export table to CSV |
| `.backup create ...` | Create backup |
| `.backup list` | List backups |
| `.metrics` | Show database metrics |
| `.status` | Show database status |
| `.vacuum` | Run VACUUM |
| `.analyze` | Run ANALYZE |
| `.help` | Show help |
| `.quit`, `.exit` | Exit CLI |

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
