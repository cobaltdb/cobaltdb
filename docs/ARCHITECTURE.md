# Architecture

## Overview

CobaltDB is a lightweight, embeddable SQL database engine written in pure Go. It follows a layered architecture with clear separation of concerns.

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Application                          │
│  (CLI, Server, Embedded usage)                            │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                     Engine (pkg/engine)                      │
│  - Database open/close                                     │
│  - Query execution                                         │
│  - Transaction management                                   │
│  - Result handling                                         │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                   Catalog (pkg/catalog)                     │
│  - Schema management (tables, indexes)                     │
│  - Data operations (CRUD)                                   │
│  - WHERE clause evaluation                                 │
│  - Disk persistence                                        │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                   Query (pkg/query)                          │
│  - Lexer (tokenization)                                    │
│  - Parser (AST generation)                                 │
│  - Expression evaluation                                   │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                  Storage (pkg/storage)                       │
│  - Buffer Pool (page caching)                              │
│  - Page Management                                         │
│  - Disk I/O                                                │
│  - WAL (Write-Ahead Log) - planned                         │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                    BTree (pkg/btree)                         │
│  - B+Tree implementation                                    │
│  - In-memory index                                         │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### Engine

The engine is the main entry point for database operations:

- `Open()` - Opens or creates a database
- `Exec()` - Executes DDL/DML statements
- `Query()` - Executes SELECT queries
- `Begin()` - Starts transactions

### Catalog

The catalog manages the database schema and data:

- **Table Management**: CREATE TABLE, DROP TABLE
- **Index Management**: CREATE INDEX
- **Data Operations**: INSERT, UPDATE, DELETE, SELECT
- **Persistence**: Save/load schema and data to disk

### Query

The query package handles SQL parsing:

- **Lexer**: Tokenizes SQL strings
- **Parser**: Builds AST from tokens
- **Expression Evaluator**: Evaluates WHERE clauses

### Storage

The storage layer handles data persistence:

- **Buffer Pool**: LRU cache for pages
- **Page Management**: Fixed-size pages
- **Backends**: Memory or disk-based

### BTree

B+Tree implementation for indexing:

- In-memory index structure
- Ordered key-value storage
- Efficient range queries

## Data Flow

### Write Path

```
User Query → Engine.Exec() → Query.Parse()
    → Catalog.Update/Insert/Delete
    → BTree.Put()
    → Buffer Pool
    → Disk (if persistent)
```

### Read Path

```
User Query → Engine.Query() → Query.Parse()
    → Catalog.Select()
    → BTree.Scan()
    → Buffer Pool
    → Return rows
```

## Transaction Support

CobaltDB supports ACID transactions:

- **Atomicity**: All operations in a transaction succeed or fail together
- **Consistency**: Database constraints are maintained
- **Isolation**: Concurrent transactions are isolated
- **Durability**: Committed data is persisted (for disk databases)

## Persistence Model

### In-Memory Mode

- All data stored in memory
- Fast but ephemeral
- Lost on restart

### Disk Mode

- Data persisted to disk
- Schema: `database.cb.data/schema.json`
- Data: `database.cb.data/table_name.json`
- Survives restart

## Performance Characteristics

- **Insert**: ~300K ops/sec (in-memory)
- **Select**: Full table scan
- **Update/Delete**: Full table scan with filtering
- **Index**: Created but not used in queries yet

## Future Improvements

See [CHANGELOG.md](../CHANGELOG.md) for roadmap:
- WAL for crash recovery
- B+Tree disk persistence
- Index usage in query execution
- Query optimizer
- JOIN support
