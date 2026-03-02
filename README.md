# 🔷 CobaltDB

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.21+-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go Version">
  <img src="https://img.shields.io/badge/Version-1.5.0-blue?style=for-the-badge" alt="Version">
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License">
  <img src="https://img.shields.io/badge/CGO-Free-ff6b6b?style=for-the-badge" alt="Zero CGO">
</p>

<p align="center">
  <b>⚡ The Modern Embedded Database for Go</b><br>
  <i>SQL + JSON · ACID Transactions · MVCC · Pure Go · Zero Dependencies</i>
</p>

---

## 🚀 What Makes CobaltDB Special

| Feature | CobaltDB | SQLite | BoltDB |
|---------|----------|--------|--------|
| **Language** | Go (Zero CGO) | C | Go |
| **Query Language** | SQL + JSONPath | SQL | Key-Value Only |
| **JSON Support** | Native | Extension | Manual |
| **Transactions** | MVCC (Snapshot Isolation) | WAL | ACID |
| **Concurrency** | Lock-Free Reads | File Locks | Lock-Free |
| **Indexes** | B+Tree | B+Tree | B+Tree |
| **Network Server** | ✅ Built-in | ❌ | ❌ |
| **Views/Triggers** | ✅ Full Support | ✅ | ❌ |

---

## 📦 Installation

```bash
go get github.com/cobaltdb/cobaltdb
```

**Requirements:** Go 1.21 or higher · Zero CGO dependencies

---

## ⚡ Quick Start

### Embedded Mode (Library)

```go
package main

import (
    "context"
    "log"
    "github.com/cobaltdb/cobaltdb/pkg/engine"
)

func main() {
    // Create in-memory database
    db, err := engine.Open(":memory:", &engine.Options{
        InMemory:  true,
        CacheSize: 1024,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    ctx := context.Background()

    // Create table with constraints
    db.Exec(ctx, `CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        metadata JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )`)

    // Insert with JSON
    db.Exec(ctx, 
        "INSERT INTO users (name, email, metadata) VALUES (?, ?, ?)",
        "Ersin Koc", 
        "ersin@cobaltdb.dev",
        `{"role": "admin", "active": true}`)

    // Query with JSON extraction
    rows, _ := db.Query(ctx, `
        SELECT name, email, JSON_EXTRACT(metadata, '$.role') as role
        FROM users 
        WHERE name = ?`, 
        "Ersin Koc")
    defer rows.Close()
}
```

### Server Mode

```bash
# Start the server
go run cmd/cobaltdb-server/main.go

# Or build and run
./cobaltdb-server --addr :8080 --data ./data
```

### CLI Mode

```bash
# Interactive shell
./cobaltdb-cli -i

# Execute SQL directly
./cobaltdb-cli -memory "SELECT * FROM users"

# Connect to server
./cobaltdb-cli -host localhost:8080
```

---

## 🔥 Performance Benchmarks

**Test Environment:** AMD Ryzen 7 PRO 6850H · Go 1.26 · Windows

| Operation | Latency | Throughput |
|-----------|---------|------------|
| **INSERT** | ~3.2 µs | **310K ops/sec** |
| **SELECT (Point Lookup)** | ~300 ns | **3.3M ops/sec** |
| **UPDATE** | ~1.06 µs | **940K ops/sec** |
| **DELETE** | ~1.6 µs | **620K ops/sec** |
| **Concurrent INSERT** | ~2.1 µs | **470K ops/sec** |
| **Transaction** | ~3.4 µs | **290K tx/sec** |

> 💡 **In-memory benchmarks.** Disk persistence adds ~20-40% overhead depending on storage.

---

## ✨ Feature Highlights

### 🗄️ SQL Support

```sql
-- DDL: Schema Definition
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL CHECK (price > 0),
    category TEXT,
    tags JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_category ON products(category);
CREATE VIEW expensive_products AS 
    SELECT * FROM products WHERE price > 100;

-- DML: Data Manipulation
INSERT INTO products (name, price, category, tags) 
VALUES ('MacBook Pro', 1999.99, 'Electronics', '["laptop", "apple"]');

-- Complex SELECT with JOINs, Aggregates, Window Functions
SELECT 
    p.category,
    COUNT(*) as total,
    AVG(p.price) as avg_price,
    MAX(p.price) as max_price,
    ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY p.price DESC) as rank
FROM products p
LEFT JOIN orders o ON p.id = o.product_id
WHERE p.price BETWEEN 100 AND 500
GROUP BY p.category
HAVING COUNT(*) > 5
ORDER BY avg_price DESC
LIMIT 10;
```

### 📊 Advanced Features

**Window Functions**
```sql
SELECT 
    name,
    salary,
    AVG(salary) OVER (PARTITION BY dept) as dept_avg,
    RANK() OVER (ORDER BY salary DESC) as salary_rank,
    LAG(salary) OVER (ORDER BY salary) as prev_salary
FROM employees;
```

**JSON Operations**
```sql
-- Extract nested values
SELECT JSON_EXTRACT(metadata, '$.user.address.city') FROM users;

-- Modify JSON
UPDATE users SET metadata = JSON_SET(metadata, '$.last_login', '2026-03-02');

-- Array operations
SELECT * FROM products WHERE JSON_ARRAY_LENGTH(tags) > 2;
```

**Transactions (ACID)**
```sql
BEGIN;
    UPDATE accounts SET balance = balance - 100 WHERE id = 1;
    UPDATE accounts SET balance = balance + 100 WHERE id = 2;
    -- Atomic transfer
COMMIT;
-- Or ROLLBACK on error
```

**Triggers & Procedures**
```sql
-- Audit logging trigger
CREATE TRIGGER audit_log 
AFTER INSERT ON users
BEGIN
    INSERT INTO audit (table_name, action, record_id, created_at)
    VALUES ('users', 'INSERT', NEW.id, CURRENT_TIMESTAMP);
END;

-- Stored procedure
CREATE PROCEDURE transfer_funds(from_id INT, to_id INT, amount REAL)
BEGIN
    UPDATE accounts SET balance = balance - amount WHERE id = from_id;
    UPDATE accounts SET balance = balance + amount WHERE id = to_id;
END;

CALL transfer_funds(1, 2, 100.00);
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      CLIENT LAYER                           │
│    Go SDK  ·  CLI  ·  TCP/Wire Protocol  ·  REST API        │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                    SQL QUERY ENGINE                         │
│  Parser → Planner → Optimizer → Executor (Iterator Model)   │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│              TRANSACTION MANAGER (MVCC)                     │
│         Snapshot Isolation · Conflict Detection             │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                   STORAGE ENGINE                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   B+Tree     │  │ Index Mgr    │  │  Buffer Pool     │  │
│  │  (Row Store) │  │ (Secondary)  │  │  (LRU Cache)     │  │
│  └──────────────┘  └──────────────┘  └──────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Page Manager · WAL (Write-Ahead Log) · Free Page List │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                      I/O LAYER                              │
│              Disk Backend  ·  Memory Backend                │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Storage** | B+Tree | Efficient range queries, ordered iteration |
| **Buffer Pool** | LRU | Page caching, reduces disk I/O |
| **WAL** | Write-Ahead Log | Durability, crash recovery |
| **Transactions** | MVCC | Lock-free reads, snapshot isolation |
| **JSON** | Native Parser | Document storage without external deps |

---

## 📋 SQL Reference

### Data Types

| Type | Description | Example |
|------|-------------|---------|
| `INTEGER` | 64-bit signed integer | `42`, `-17` |
| `REAL` | 64-bit floating point | `3.14159`, `-0.001` |
| `TEXT` | Variable-length string | `'hello'`, `"world"` |
| `BOOLEAN` | True/False | `TRUE`, `FALSE` |
| `JSON` | Native JSON document | `'{"key": "value"}'` |
| `DATE` | Date only | `'2026-03-02'` |
| `TIMESTAMP` | Date + Time | `'2026-03-02 14:30:00'` |

### SQL Functions

**String:** `LENGTH`, `UPPER`, `LOWER`, `TRIM`, `SUBSTR`, `CONCAT`, `REPLACE`, `INSTR`  
**Numeric:** `ABS`, `ROUND`, `FLOOR`, `CEIL`  
**Aggregate:** `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`  
**JSON:** `JSON_EXTRACT`, `JSON_SET`, `JSON_REMOVE`, `JSON_VALID`, `JSON_ARRAY_LENGTH`, `JSON_MERGE`  
**Window:** `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE`  
**Date/Time:** `DATE`, `TIME`, `DATETIME`, `STRFTIME`  
**Utility:** `COALESCE`, `IFNULL`, `NULLIF`, `CAST`

### Supported Statements

```sql
-- DDL
CREATE TABLE ... [PRIMARY KEY] [NOT NULL] [UNIQUE] [CHECK] [FOREIGN KEY]
CREATE INDEX ... ON ...
CREATE VIEW ... AS SELECT ...
CREATE TRIGGER ... BEFORE|AFTER INSERT|UPDATE|DELETE
CREATE PROCEDURE ...
DROP TABLE|INDEX|VIEW|TRIGGER|PROCEDURE ... [IF EXISTS]

-- DML
INSERT INTO ... VALUES (...)
INSERT INTO ... SELECT ...
UPDATE ... SET ... WHERE ...
DELETE FROM ... WHERE ...
SELECT ... FROM ... [JOIN ... ON ...] [WHERE ...] [GROUP BY ...] [HAVING ...] [ORDER BY ...] [LIMIT ...] [OFFSET ...]

-- DCL
BEGIN | COMMIT | ROLLBACK
CALL procedure_name(...)
```

---

## 🔧 Development

```bash
# Clone the repository
git clone https://github.com/cobaltdb/cobaltdb.git
cd cobaltdb

# Run tests
go test ./... -v

# Run benchmarks
go test -bench=. -benchtime=2s ./test/...

# Build CLI
go build -o cobaltdb-cli ./cmd/cobaltdb-cli

# Build Server
go build -o cobaltdb-server ./cmd/cobaltdb-server

# Run demo
go run cmd/demo/main.go
```

---

## 📚 Documentation

- [Architecture](docs/ARCHITECTURE.md) - System design & components
- [API Reference](docs/API.md) - Go SDK documentation  
- [SQL Reference](docs/SQL.md) - Complete SQL syntax
- [Benchmarks](docs/BENCHMARKS.md) - Performance metrics
- [Changelog](CHANGELOG.md) - Version history

---

## 🛣️ Roadmap

- [ ] **v1.6** - Common Table Expressions (WITH clause), VACUUM/ANALYZE
- [ ] **v2.0** - Full-Text Search (FTS), Materialized Views
- [ ] **v2.5** - Replication, User Management & Permissions
- [ ] **v3.0** - Distributed mode, Sharding support

---

## 💪 Why CobaltDB?

1. **🚀 Pure Go** - Zero CGO means easy cross-compilation and deployment
2. **📱 Embedded or Server** - Use as library or standalone server
3. **🔄 ACID + MVCC** - True isolation without locking reads
4. **🗂️ SQL + JSON** - Relational power with document flexibility
5. **⚡ Blazing Fast** - 3.3M+ point lookups per second
6. **🛠️ Production Ready** - Comprehensive test coverage, battle-tested

---

## 📄 License

MIT License - see [LICENSE](LICENSE) file.

---

<p align="center">
  <b>Built with ❤️ by Ersin KOÇ</b><br>
  <a href="https://github.com/cobaltdb/cobaltdb">GitHub</a> · 
  <a href="https://pkg.go.dev/github.com/cobaltdb/cobaltdb">Go Reference</a> · 
  <a href="https://goreportcard.com/report/github.com/cobaltdb/cobaltdb">Report Card</a>
</p>
