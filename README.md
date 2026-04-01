# 🔷 CobaltDB

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.24+-00ADD8?style=for-the-badge&logo=go&logoColor=white" alt="Go Version">
  <img src="https://img.shields.io/badge/Version-0.3.1-blue?style=for-the-badge" alt="Version">
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License">
  <img src="https://img.shields.io/badge/CGO-Free-ff6b6b?style=for-the-badge" alt="Zero CGO">
  <img src="https://img.shields.io/badge/Coverage-92%25-brightgreen?style=for-the-badge" alt="Test Coverage">
  <img src="https://img.shields.io/badge/Production-Ready-success?style=for-the-badge" alt="Production Ready">
</p>

<p align="center">
  <b>⚡ The Modern SQL Database Engine — Embedded or Standalone Server</b><br>
  <i>MySQL Protocol · SQL + JSON · ACID · MVCC · Encryption · Replication · Pure Go</i>
</p>

---

## 🚀 Two Modes, One Database

CobaltDB runs in two modes — use it as an **embedded library** inside your Go application, or deploy it as a **standalone database server** that any MySQL client can connect to.

```
┌─────────────────────────────────────────────────────────┐
│                     CobaltDB                            │
│                                                         │
│  ┌─────────────┐           ┌───────────────────────┐    │
│  │ Embedded    │           │ Standalone Server     │    │
│  │ (Go Library)│           │ (MySQL Protocol)      │    │
│  │             │           │                       │    │
│  │ db.Query()  │           │ mysql -h host -P 4200 │    │
│  │ db.Exec()   │           │ Any MySQL client/ORM  │    │
│  └─────────────┘           └───────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Comparison

| Feature | CobaltDB | PostgreSQL | MySQL | SQLite |
|---------|----------|------------|-------|--------|
| **Deployment** | Embedded + Server | Server only | Server only | Embedded only |
| **Protocol** | MySQL wire protocol | PostgreSQL | MySQL | C API |
| **Language** | Pure Go (Zero CGO) | C | C/C++ | C |
| **Query Language** | SQL + JSON | SQL + JSON | SQL + JSON | SQL |
| **Encryption at Rest** | ✅ AES-256-GCM | Plugin | Plugin | ❌ |
| **WAL Encryption** | ✅ Built-in | ❌ | ❌ | ❌ |
| **TLS** | ✅ TLS 1.2+ | ✅ | ✅ | ❌ |
| **Replication** | ✅ Master-Slave | ✅ | ✅ | ❌ |
| **Row-Level Security** | ✅ Policy-based | ✅ | ❌ | ❌ |
| **Audit Logging** | ✅ Encrypted | Plugin | Plugin | ❌ |
| **Vector Search** | ✅ HNSW | pgvector | ❌ | ❌ |
| **Temporal Queries** | ✅ AS OF | ✅ | ❌ | ❌ |
| **Zero Dependencies** | ✅ | ❌ | ❌ | ✅ |
| **Cross-Compile** | ✅ Any OS/Arch | ❌ | ❌ | CGO needed |
| **Single Binary** | ✅ | ❌ | ❌ | Library |

---

## 📦 Installation

```bash
go get github.com/cobaltdb/cobaltdb
```

**Requirements:** Go 1.24+ (`toolchain go1.26.1`) · Zero CGO runtime dependency

### Verification and Security Checks

Run core checks locally:

```bash
make verify
```

Run full security/concurrency gate (requires CGO toolchain and `gcc` for race detector):

```bash
make verify-security
```

If tool commands are missing locally, install:

```bash
go install golang.org/x/vuln/cmd/govulncheck@latest
go install github.com/securego/gosec/v2/cmd/gosec@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

---

## ⚡ Quick Start

### 1. Server Mode — Connect with Any MySQL Client

```bash
# Start CobaltDB server
./cobaltdb-server --mysql-addr 127.0.0.1:3307 --admin-pass "StrongPass123!" --data ./mydb.db

# Connect with standard MySQL client
mysql -h 127.0.0.1 -P 3307 -u admin -p
```

```sql
mysql> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);
mysql> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
mysql> SELECT * FROM users;
+----+-------+-------------------+
| id | name  | email             |
+----+-------+-------------------+
|  1 | Alice | alice@example.com |
+----+-------+-------------------+
```

> **Verified working** with `go-sql-driver/mysql`, `mysql` CLI, and standard MySQL wire protocol.
> Works with **any MySQL-compatible client**: Python (mysql-connector, SQLAlchemy), Node.js (mysql2, Prisma), Java (JDBC), Ruby, PHP, etc.

**Server features:** MySQL protocol, TLS 1.2+, authentication (Argon2id), connection pooling, rate limiting, circuit breaker, health checks, encrypted audit logging, master-slave replication.

### 2. Embedded Mode — Use as a Go Library

```go
import "github.com/cobaltdb/cobaltdb/pkg/engine"

db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})
defer db.Close()

ctx := context.Background()
db.Exec(ctx, `CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    metadata JSON
)`)

db.Exec(ctx, "INSERT INTO users VALUES (?, ?, ?, ?)",
    1, "Alice", "alice@example.com", `{"role": "admin"}`)

rows, _ := db.Query(ctx, `SELECT name, JSON_EXTRACT(metadata, '$.role') FROM users`)
```

> Zero CGO, zero dependencies. Import and use — no external server needed.

### CLI Mode

```bash
# Interactive shell
./cobaltdb-cli -i

# Execute SQL directly
./cobaltdb-cli -memory "SELECT * FROM users"

# Connect to running server
./cobaltdb-cli -host localhost:4200
```

### Docker Mode

```bash
# Start with Docker Compose (includes Prometheus + Grafana monitoring)
docker-compose up -d

# Or run standalone
docker build -t cobaltdb .
docker run -d -p 4200:4200 -v cobaltdb_data:/data/cobaltdb cobaltdb

# Connect to containerized database
cobaltdb-cli -host localhost:4200
```

See [DOCKER.md](DOCKER.md) for detailed Docker setup instructions.

---

## 🌍 Multi-Language SDKs

CobaltDB speaks the MySQL wire protocol — connect from **any language** using standard MySQL drivers.

### Go (Embedded)
```go
import (
    "database/sql"
    _ "github.com/cobaltdb/cobaltdb/sdk/go"  // register driver
)

db, _ := sql.Open("cobaltdb", "file://./data/mydb.cb?cache=1024")
db.Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
db.Exec("INSERT INTO users VALUES (?, ?)", 1, "Alice")
```

### Go (Network Client)
```go
import "database/sql"
import _ "github.com/go-sql-driver/mysql"

db, _ := sql.Open("mysql", "admin@tcp(127.0.0.1:3307)/")
rows, _ := db.Query("SELECT * FROM users")
```

### Python
```python
import cobaltdb  # sdk/python

conn = cobaltdb.connect(host='127.0.0.1', port=3307, user='admin')
cursor = conn.execute("SELECT * FROM users")
for row in cursor.fetchall():
    print(row)
```

### Node.js
```javascript
const cobaltdb = require('./sdk/js');

const conn = await cobaltdb.connect({ host: '127.0.0.1', port: 3307 });
const [rows] = await conn.execute('SELECT * FROM users');
console.log(rows);
```

### Java
```java
import com.cobaltdb.sdk.CobaltDB;

Connection conn = CobaltDB.connect("127.0.0.1", 3307, "admin", "");
ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM users");
```

### Any MySQL Client
```bash
mysql -h 127.0.0.1 -P 3307 -u admin -e "SELECT * FROM users"
```

> **Tip:** Any MySQL-compatible ORM works too — SQLAlchemy, Prisma, Hibernate, GORM, Sequelize, ActiveRecord, etc.

---

## 🔥 Performance Benchmarks

**Test Environment:** AMD Ryzen 9 9950X3D · Go 1.26 · Windows 11

### Core Operations (B-Tree Level)

| Operation | Latency | Throughput |
|-----------|---------|------------|
| **PUT** | ~641 ns | **1.56M ops/sec** |
| **PUT (Sequential)** | ~694 ns | **1.44M ops/sec** |
| **GET (Point Lookup)** | ~64 ns | **15.7M ops/sec** |
| **UPDATE** | ~153 ns | **6.5M ops/sec** |
| **DELETE** | ~197 ns | **5.1M ops/sec** |
| **SCAN (1K range)** | ~270 µs | **3.7K ops/sec** |

### SQL Engine Performance (10K rows)

| Operation | Latency | Detail |
|-----------|---------|--------|
| **INSERT** | ~2.0 µs | Single row with SQL parsing |
| **Point Lookup** | ~2.1 µs | WHERE id = ? (indexed) |
| **Full Scan (1K)** | ~598 µs | Custom fast decoder, no reflection |
| **Full Scan (10K)** | ~8.8 ms | 130K allocs (42% less than json.Unmarshal) |
| **SUM/AVG** | ~5.0 ms | Byte-level fast path, no JSON decode |
| **COUNT(*)** | ~4.4 ms | Fast path, skip row decode |
| **LIMIT 100 OFFSET 1K** | ~3.7 ms | Early termination |
| **WHERE (10K)** | ~10.3 ms | Custom decoder + expression eval |
| **Index vs No Index** | 458 µs vs 8.7 ms | **19x faster with index** |
| **Inner JOIN (1K)** | ~724 µs | Hash join |
| **3-Way JOIN (1K×3)** | ~2.0 ms | Hash join |
| **Recursive CTE** | ~3.6 µs | 1000 nodes |
| **Simple CTE** | ~584 µs | View-based resolution |
| **Concurrent Read (×20)** | ~669 ns | Parallel goroutines |
| **Transaction** | ~347 µs | Single statement |
| **Rollback** | ~167 µs | 100 statements |
| **Window (RowNumber)** | ~10.0 ms | OVER (ORDER BY) on 10K rows |
| **DELETE (bulk)** | ~998 µs | WHERE age < 50 on 1K rows |
| **UPDATE (bulk)** | ~9.2 ms | WHERE age < 50 on 10K rows |

### Parser & Storage Performance

| Component | Operation | Latency | Throughput |
|-----------|-----------|---------|------------|
| **SQL Parser** | Parse SELECT | ~826 ns | **1.2M ops/sec** |
| **SQL Parser** | Parse INSERT | ~1.0 µs | **960K ops/sec** |
| **SQL Parser** | Parse Complex Query | ~4.7 µs | **214K ops/sec** |
| **Lexer** | Tokenize | ~499 ns | **2.0M ops/sec** |
| **Buffer Pool** | Get Page | ~27 ns | **36.5M ops/sec** |
| **Buffer Pool** | Memory Read | ~34 ns | **29.4M ops/sec** |
| **WAL** | Append | ~192 µs | **5.2K ops/sec** |

> 💡 **In-memory benchmarks.** Disk persistence adds ~20-40% overhead depending on storage.

---

## 🔐 Security Features

CobaltDB provides enterprise-grade security features:

### Encryption at Rest

```go
import "github.com/cobaltdb/cobaltdb/pkg/storage"

// Generate encryption key
key, _ := storage.GenerateSecureKey()

// Open encrypted database
db, _ := engine.Open("encrypted.db", &engine.Options{
    EncryptionKey: key,
})
```

- **AES-256-GCM** authenticated encryption
- **Argon2id** for secure key derivation
- Transparent encryption/decryption

### TLS Support

```go
import "github.com/cobaltdb/cobaltdb/pkg/server"

config := &server.Config{
    Address: ":4200",
    TLS: &server.TLSConfig{
        Enabled:              true,
        GenerateSelfSigned:   true,  // Auto-generate certs
        // Or provide your own:
        // CertFile: "server.crt",
        // KeyFile:  "server.key",
    },
}

srv, _ := server.New(db, config)
srv.Listen(":4200", config.TLS)
```

- **TLS 1.2/1.3** support
- Self-signed certificate generation
- Client certificate authentication

### Audit Logging

```go
import "github.com/cobaltdb/cobaltdb/pkg/audit"

auditConfig := &audit.Config{
    Enabled:  true,
    LogFile:  "audit.log",
    LogFormat: "json",  // or "text"
}

db, _ := engine.Open("audited.db", &engine.Options{
    AuditConfig: auditConfig,
})
```

- JSON and text format support
- Query, DDL, and authentication events
- Automatic log rotation (100MB default)

### Row-Level Security (RLS)

```go
import "github.com/cobaltdb/cobaltdb/pkg/security"

// Enable RLS
db, _ := engine.Open("secure.db", &engine.Options{
    EnableRLS: true,
})

// Create policies via SQL
// CREATE POLICY tenant_isolation ON users
//   USING (tenant_id = current_tenant());
```

---

## 🏭 Production Features

CobaltDB includes enterprise-grade production features for resilience, observability, and high availability:

### Circuit Breaker

```go
import "github.com/cobaltdb/cobaltdb/pkg/engine"

config := &engine.CircuitBreakerConfig{
    MaxFailures:         5,
    MinSuccesses:        3,
    ResetTimeout:        30 * time.Second,
    MaxConcurrency:      100,
    HalfOpenMaxRequests: 1,
}

cb := engine.NewCircuitBreaker(config)

if err := cb.Allow(); err != nil {
    return err // Circuit open
}
defer cb.Release()

err := doOperation()
if err != nil {
    cb.ReportFailure()
} else {
    cb.ReportSuccess()
}
```

- Three states: Closed, Open, Half-Open
- Automatic recovery with configurable timeout
- Concurrency control and rate limiting in half-open state

### Retry Logic

```go
config := &engine.RetryConfig{
    MaxAttempts:  3,
    InitialDelay: 100 * time.Millisecond,
    MaxDelay:     30 * time.Second,
    Multiplier:   2.0,
    Jitter:       0.1, // 10% randomization
}

err := engine.Retry(ctx, config, func() error {
    return db.Query("SELECT * FROM users")
})

// Or with result
result, err := engine.RetryWithResult(ctx, config, func() (string, error) {
    return fetchData()
})
```

- Exponential backoff with jitter
- Context cancellation support
- 4 predefined policies: Fast, Standard, Aggressive, Background

### Rate Limiter

```go
import "github.com/cobaltdb/cobaltdb/pkg/server"

config := &server.RateLimiterConfig{
    RPS:             1000,
    Burst:           100,
    PerClient:       true,
    CleanupInterval: 5 * time.Minute,
    MaxClients:      10000,
}

rl := server.NewRateLimiter(config)
defer rl.Stop()

if !rl.Allow("client-id") {
    return errors.New("rate limit exceeded")
}
```

- Token bucket algorithm
- Global and per-client rate limiting
- Adaptive rate limiting based on system load

### SQL Injection Protection

```go
config := &server.SQLProtectionConfig{
    Enabled:             true,
    BlockOnDetection:    true,
    MaxQueryLength:      10000,
    MaxORConditions:     10,
    MaxUNIONCount:       5,
    SuspiciousThreshold: 3,
}

sp := server.NewSQLProtector(config)

result := sp.CheckSQL(sql)
if !result.Allowed {
    return errors.New("SQL injection detected")
}
```

- 15 SQL injection pattern detection
- UNION-based, time-based blind, conditional blind, OOB exfil detection
- Whitelist support for trusted queries

### Distributed Tracing

```go
// Generate request ID
ctx = server.ContextWithRequestID(ctx, server.NewRequestContext().ID)

// Extract from context
requestID := server.RequestIDFromContext(ctx)
```

- Request ID tracking across components
- Span-based tracing
- Context propagation

### Health Checks & Monitoring

```bash
# Liveness probe (Kubernetes)
curl http://localhost:8420/health

# Readiness probe (Kubernetes)
curl http://localhost:8420/ready

# Detailed health status
curl http://localhost:8420/healthz

# Circuit breaker statistics
curl http://localhost:8420/circuit-breakers

# Rate limiter statistics
curl http://localhost:8420/rate-limits

# System statistics
curl http://localhost:8420/stats
```

### Production Server

```go
config := &server.ProductionConfig{
    Lifecycle: &server.LifecycleConfig{
        ShutdownTimeout: 30 * time.Second,
        DrainTimeout:    10 * time.Second,
    },
    EnableCircuitBreaker: true,
    CircuitBreaker:       engine.DefaultCircuitBreakerConfig(),
    EnableRetry:          true,
    Retry:                engine.DefaultRetryConfig(),
    EnableRateLimiter:    true,
    EnableSQLProtection:  true,
    EnableHealthServer:   true,
    HealthAddr:           ":8420",
}

ps := server.NewProductionServer(db, config)
if err := ps.Start(); err != nil {
    log.Fatal(err)
}

ps.Wait() // Wait for shutdown signal
```

- Graceful shutdown with configurable timeouts
- Signal handling for SIGTERM/SIGINT
- Component lifecycle management

---

## 📁 Project Structure

```
cobaltdb/
├── 📂 cmd/                     # Command-line tools
│   ├── cobaltdb-server/        # Production server
│   ├── cobaltdb-cli/           # Interactive CLI
│   ├── cobaltdb-migrate/       # Migration tool
│   ├── cobaltdb-bench/         # Benchmark tool
│   └── demo*/                  # Demo applications
│
├── 📂 pkg/                     # Core packages
│   ├── engine/                 # Database engine (CB, retry)
│   ├── catalog/                # SQL execution layer
│   │   ├── catalog_core.go     # Core types & helpers
│   │   ├── catalog_insert.go   # INSERT operations
│   │   ├── catalog_update.go   # UPDATE operations
│   │   ├── catalog_delete.go   # DELETE operations
│   │   ├── catalog_select.go   # SELECT & JOIN
│   │   ├── catalog_aggregate.go # GROUP BY & aggregates
│   │   ├── catalog_window.go   # Window functions
│   │   ├── catalog_cte.go      # CTE operations
│   │   ├── catalog_ddl.go      # DDL operations
│   │   ├── catalog_txn.go      # Transactions
│   │   ├── catalog_eval.go     # Expression evaluation
│   │   ├── catalog_index.go    # Index operations
│   │   ├── catalog_rls.go      # Row-Level Security
│   │   └── ...
│   ├── query/                  # SQL parser & optimizer
│   ├── btree/                  # B+Tree storage engine
│   ├── storage/                # Storage layer (WAL, buffer pool)
│   ├── server/                 # Network server (TLS, auth)
│   ├── security/               # RLS & security
│   ├── audit/                  # Audit logging
│   ├── auth/                   # Authentication
│   ├── protocol/               # MySQL protocol
│   ├── metrics/                # Metrics collection
│   └── txn/                    # Transaction manager
│
├── 📂 test/                    # Integration tests (5,000+)
├── 📂 docs/                    # Documentation
├── 📂 scripts/                 # Utility scripts
│
├── 📂 sdk/                     # Multi-language SDKs
│   ├── go/                     # Go SDK (database/sql driver)
│   ├── python/                 # Python SDK (mysql-connector wrapper)
│   ├── js/                     # Node.js SDK (mysql2 wrapper)
│   └── java/                   # Java SDK (JDBC wrapper)
│
└── 📂 website/                 # Project website
```

### Module Organization

| Package | Purpose | Lines of Code |
|---------|---------|---------------|
| `pkg/catalog` | SQL execution (18 files) | ~8,500 |
| `pkg/query` | SQL parser & optimizer | ~6,000 |
| `pkg/btree` | B+Tree storage | ~1,500 |
| `pkg/storage` | Storage layer | ~2,500 |
| `pkg/server` | Network & production | ~3,500 |
| `pkg/engine` | Resilience (CB, retry) | ~800 |

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
| `VECTOR(n)` | n-dimensional vector | `VECTOR(128)` for embeddings |
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

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out

# Run benchmarks
go test -bench=. -benchtime=2s ./test/...

# Build CLI
go build -o cobaltdb-cli ./cmd/cobaltdb-cli

# Build Server
go build -o cobaltdb-server ./cmd/cobaltdb-server

# Run demo
go run cmd/demo/main.go
```

### Test Coverage

| Package | Coverage | Package | Coverage |
|---------|----------|---------|----------|
| `pkg/pool` | 98.0% ✅ | `pkg/wasm` | 93.4% ✅ |
| `pkg/auth` | 96.8% ✅ | `pkg/btree` | 92.4% ✅ |
| `pkg/cache` | 95.5% ✅ | `pkg/backup` | 91.9% ✅ |
| `pkg/protocol` | 95.1% ✅ | `pkg/security` | 91.9% ✅ |
| `pkg/metrics` | 94.8% ✅ | `pkg/replication` | 91.8% ✅ |
| `pkg/wire` | 94.7% ✅ | `pkg/query` | 90.9% ✅ |
| `pkg/optimizer` | 93.8% ✅ | `pkg/audit` | 90.9% ✅ |
| `pkg/logger` | 93.8% ✅ | `pkg/storage` | 90.5% ✅ |
| `pkg/txn` | 93.5% ✅ | `pkg/server` | 90.2% ✅ |
| `pkg/engine` | 90.0% ✅ | `pkg/catalog` | 85.5% ✅ |

> **10,400+ tests** across 22 packages, all passing. 19/20 packages above 90% coverage.

---

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [CHANGELOG.md](CHANGELOG.md) | Version history, all changes |
| [COVERAGE_GUIDE.md](COVERAGE_GUIDE.md) | Test coverage analysis and targets |
| [FEATURES.md](FEATURES.md) | **Feature status - what works 100% vs partially** |
| [docs/PRODUCTION.md](docs/PRODUCTION.md) | **Production features guide (Circuit Breaker, Retry, Rate Limiting)** |
| [docs/ARCHITECTURE_FULL.md](docs/ARCHITECTURE_FULL.md) | System design & components |
| [docs/API.md](docs/API.md) | Go SDK documentation |
| [docs/SQL.md](docs/SQL.md) | Complete SQL syntax |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | Performance benchmarks |
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | Getting started guide |

---

## 🛣️ Roadmap

### ✅ Completed Features (v0.1.51)

- [x] **SQL Support** - SELECT, INSERT, UPDATE, DELETE with JOINs, GROUP BY, ORDER BY, LIMIT
- [x] **Window Functions** - ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE, LAST_VALUE
- [x] **JSON Support** - Native JSON type with JSON_EXTRACT, JSON_SET, JSON_REMOVE, JSON_ARRAY_LENGTH
- [x] **Indexes** - CREATE INDEX, DROP INDEX with B+Tree implementation
- [x] **Views** - CREATE VIEW, DROP VIEW support
- [x] **Triggers** - CREATE TRIGGER with BEFORE/AFTER, INSERT/UPDATE/DELETE events
- [x] **Stored Procedures** - CREATE PROCEDURE, CALL support
- [x] **Transactions** - BEGIN, COMMIT, ROLLBACK with ACID compliance
- [x] **User Management** - Authentication with permissions and sessions
- [x] **Constraints** - PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL
- [x] **MySQL Protocol** - Wire-compatible MySQL protocol support

### ✅ Completed Features (v0.2.0)

- [x] **Full-Text Search** - MATCH ... AGAINST syntax with inverted indexes
- [x] **Materialized Views** - CREATE MATERIALIZED VIEW, REFRESH, and DROP support
- [x] **Common Table Expressions** - WITH clause support for recursive and non-recursive CTEs
- [x] **VACUUM** - Database compaction and storage reclamation
- [x] **ANALYZE** - Table statistics collection for query optimization

### ✅ Production Hardening (v0.2.11)

- [x] **Panic Recovery** - Server survives any query panic
- [x] **Resource Leak Fixes** - All iterators properly closed
- [x] **Race Condition Fixes** - Statement cache thread-safety
- [x] **Transaction Fixes** - Connection leak plugged
- [x] **Data Corruption Fix** - Free list loading corrected

### ✅ Enterprise Production Features (v0.2.20)

- [x] **Circuit Breaker** - Three-state breaker with automatic recovery
- [x] **Retry Logic** - Exponential backoff with 4 policies
- [x] **Rate Limiter** - Token bucket with adaptive limiting
- [x] **SQL Injection Protection** - 10+ pattern detection
- [x] **Distributed Tracing** - Request ID tracking
- [x] **Graceful Shutdown** - Signal handling with drain timeout
- [x] **Health Checks** - Kubernetes-compatible probes

### ✅ v0.2.22 - WASM & Advanced Features (2026-03-17)

- [x] **WASM Compilation** - Compile SQL queries to WebAssembly bytecode
- [x] **Query Plan Cache** - LRU cache for parsed query plans with statistics
- [x] **Vector Support** - VECTOR data type with HNSW index for similarity search
- [x] **Temporal Queries** - AS OF SYSTEM TIME for time-travel queries

### ✅ v0.3.0 - Security Hardening & Stability (2026-03-20)

- [x] **WAL Encryption** - AEAD encryption for write-ahead log with header authentication
- [x] **Audit Log Encryption** - AES-256-GCM encrypted audit log entries
- [x] **RLS Hardening** - Fixed bypass in UPDATE...FROM and DELETE...USING
- [x] **Auth Hardening** - Password policy, brute force rate limiting, random default password
- [x] **SQL Injection Protection** - 15 detection patterns (conditional blind, OOB exfil, etc.)
- [x] **Concurrency Fixes** - Panic recovery, double-close protection, lifecycle tracking
- [x] **10,400+ Tests** - 19/20 packages above 90% coverage

### ✅ v0.3.1 - Production Ready Release (2026-03-31)

- [x] **Deadlock Detection** - Wait-for graph with automatic cycle detection and resolution
- [x] **Transaction Timeout** - Configurable per-transaction and lock wait timeouts
- [x] **Transaction Metrics** - Real-time monitoring of active, committed, aborted transactions
- [x] **Chaos Engineering** - Comprehensive stress tests for production readiness
- [x] **Lock Management** - Fine-grained lock tracking with automatic release
- [x] **Production Readiness Score: 93.5/100**
- [x] **Graceful Shutdown** - Signal handling with drain timeout
- [x] **Health Checks** - Kubernetes-compatible probes

### ✅ v0.2.22 - WASM & Advanced Features (2026-03-17)

- [x] **WASM Compilation** - Compile SQL queries to WebAssembly bytecode
- [x] **Query Plan Cache** - LRU cache for parsed query plans with statistics
- [x] **Vector Support** - VECTOR data type with HNSW index for similarity search
- [x] **Temporal Queries** - AS OF SYSTEM TIME for time-travel queries

### ✅ v0.3.0 - Security Hardening & Stability (2026-03-20)

- [x] **WAL Encryption** - AEAD encryption for write-ahead log with header authentication
- [x] **Audit Log Encryption** - AES-256-GCM encrypted audit log entries
- [x] **RLS Hardening** - Fixed bypass in UPDATE...FROM and DELETE...USING
- [x] **Auth Hardening** - Password policy, brute force rate limiting, random default password
- [x] **SQL Injection Protection** - 15 detection patterns (conditional blind, OOB exfil, etc.)
- [x] **Concurrency Fixes** - Panic recovery, double-close protection, lifecycle tracking
- [x] **10,400+ Tests** - 19/20 packages above 90% coverage

### 📋 Planned Features

- [ ] **v0.4.0** - Distributed mode, Sharding support
- [ ] **v0.5.0** - Cloud-native features, Kubernetes operator

---

## 💪 Why CobaltDB?

1. **🚀 Pure Go** - Zero CGO, single binary, cross-compile to any OS/architecture
2. **📱 Embedded + Server** - Use as Go library OR deploy as standalone MySQL-compatible server
3. **🔒 Security First** - AES-256-GCM encryption (data + WAL + audit), TLS 1.2+, RLS, Argon2id auth
4. **🔄 ACID + MVCC** - Snapshot isolation, lock-free reads, WAL durability
5. **🗂️ SQL + JSON + Vector** - Relational queries, JSONPath, HNSW similarity search
6. **⚡ Blazing Fast** - 15M+ point lookups/sec, 1.5M+ inserts/sec
7. **🏭 Production Ready** - 10,400+ tests, 92% coverage, circuit breaker, rate limiter, replication

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
