# Getting Started with CobaltDB

Welcome to CobaltDB! This guide will help you get up and running quickly.

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Using the CLI](#using-the-cli)
4. [Go SDK](#go-sdk)
5. [Docker Setup](#docker-setup)
6. [Configuration](#configuration)
7. [Monitoring](#monitoring)
8. [Backup & Restore](#backup--restore)
9. [Next Steps](#next-steps)

---

## Installation

### Option 1: Download Pre-built Binaries

Download the latest release from the releases page:

```bash
# Linux/macOS
curl -L https://github.com/cobaltdb/cobaltdb/releases/latest/download/cobaltdb-linux-amd64.tar.gz | tar xz

# Windows
# Download cobaltdb-windows-amd64.zip and extract
```

### Option 2: Build from Source

```bash
git clone https://github.com/cobaltdb/cobaltdb.git
cd cobaltdb
go build -o cobaltdb-server ./cmd/cobaltdb-server
go build -o cobaltdb-cli ./cmd/cobaltdb-cli
```

### Option 3: Docker

```bash
docker pull cobaltdb/cobaltdb:latest
```

---

## Quick Start

### 1. Start the Server

```bash
# Create data directory
mkdir -p ./data

# Start server
cobaltdb-server --data-dir ./data
```

The server will start on port `4200` by default.

### 2. Connect with CLI

```bash
# In a new terminal
cobaltdb-cli

# Or connect to remote server
cobaltdb-cli --host localhost --port 4200
```

### 3. Run Your First Query

```sql
-- Create a table
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert data
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');

-- Query data
SELECT * FROM users;

-- Update
UPDATE users SET name = 'Alice Smith' WHERE id = 1;

-- Delete
DELETE FROM users WHERE id = 2;
```

---

## Using the CLI

### Connection Options

```bash
# Connect with flags
cobaltdb-cli --host localhost --port 4200 --database mydb

# Connect with DSN
cobaltdb-cli "host=localhost port=4200 database=mydb"
```

### Interactive Commands

```sql
-- Show all tables
.tables

-- Show table schema
.schema users

-- Show query plan
EXPLAIN SELECT * FROM users WHERE id = 1;

-- Enable timing
.timer on

-- Exit
.quit
```

---

## Go SDK

### Installation

```bash
go get github.com/cobaltdb/cobaltdb/sdk/go
```

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/cobaltdb/cobaltdb/sdk/go"
)

func main() {
    // Open connection
    cfg := &cobaltdb.Config{
        Host:     "localhost",
        Port:     4200,
        Database: "myapp",
    }

    db, err := cobaltdb.Open(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Execute query
    ctx := context.Background()
    result, err := db.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT
        )
    `)
    if err != nil {
        log.Fatal(err)
    }

    // Insert data
    result, err = db.Exec(ctx,
        "INSERT INTO users (name, email) VALUES (?, ?)",
        "Alice", "alice@example.com",
    )
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Inserted row with ID: %d\n", result.LastInsertID)

    // Query data
    rows, err := db.Query(ctx, "SELECT id, name, email FROM users")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var id int
        var name, email string
        if err := rows.Scan(&id, &name, &email); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("User: %d, %s, %s\n", id, name, email)
    }
}
```

### Using with database/sql

```go
import (
    "database/sql"
    _ "github.com/cobaltdb/cobaltdb/sdk/go"
)

func main() {
    db, err := sql.Open("cobaltdb", "host=localhost port=4200 database=mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Use standard database/sql API
    rows, err := db.Query("SELECT * FROM users")
    // ...
}
```

---

## Docker Setup

### Quick Start with Docker

```bash
# Run single container
docker run -d \
    --name cobaltdb \
    -p 4200:4200 \
    -p 8420:8420 \
    -v $(pwd)/data:/data/cobaltdb \
    cobaltdb/cobaltdb:latest
```

### Full Stack with Docker Compose

```bash
# Clone repository
git clone https://github.com/cobaltdb/cobaltdb.git
cd cobaltdb

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f cobaltdb

# Stop all
docker-compose down
```

### Services Included

- **cobaltdb**: Database server (port 4200)
- **prometheus**: Metrics collection (port 9090)
- **grafana**: Visualization (port 3000)
- **backup**: Automated backups

---

## Configuration

### Configuration File

Create `cobaltdb.conf`:

```toml
[server]
host = "0.0.0.0"
port = 4200
max_connections = 100

[storage]
data_dir = "/data/cobaltdb"
cache_size = 1024
wal_enabled = true

[performance]
query_plan_cache_enabled = true
query_plan_cache_size = 1000

[security]
auth_enabled = false

[monitoring]
metrics_enabled = true
metrics_port = 8420
slow_query_enabled = true
slow_query_threshold = "1s"

[logging]
log_level = "info"
log_format = "json"
```

### Environment Variables

All config options can be set via environment variables:

```bash
export COBALTDB_SERVER_PORT=4200
export COBALTDB_STORAGE_DATA_DIR=/data/cobaltdb
export COBALTDB_SECURITY_AUTH_ENABLED=true
```

---

## Monitoring

### Metrics Endpoints

- **Prometheus**: `http://localhost:8420/metrics/prometheus`
- **JSON**: `http://localhost:8420/metrics/json`
- **Health**: `http://localhost:8420/health`
- **Ready**: `http://localhost:8420/ready`

### Key Metrics

| Metric | Description |
|--------|-------------|
| `cobaltdb_queries_total` | Total queries executed |
| `cobaltdb_query_duration_seconds` | Query latency histogram |
| `cobaltdb_connections_active` | Active connections |
| `cobaltdb_cache_hits_total` | Cache hit count |
| `cobaltdb_transactions_total` | Transaction count |

### Grafana Dashboard

Access Grafana at `http://localhost:3000` (admin/admin)

Pre-built dashboards:
- **Overview**: Key metrics and health
- **Performance**: Query performance and latency
- **Resources**: Memory and cache usage

---

## Backup & Restore

### Automated Backups

```bash
# Configure in cobaltdb.conf
[backup]
backup_enabled = true
backup_schedule = "0 2 * * *"  # Daily at 2 AM
backup_retention_days = 7
```

### Manual Backup

```bash
# Using CLI
cobaltdb-cli backup create --output /backups/mydb-$(date +%Y%m%d).db

# Using Docker
docker exec cobaltdb cobaltdb-cli backup create --output /backups/backup.db
```

### Restore

```bash
# Stop server
cobaltdb-cli admin stop

# Restore
cobaltdb-cli restore --from /backups/mydb-20240101.db --to ./data

# Start server
cobaltdb-server --data-dir ./data
```

### Point-in-Time Recovery

```bash
# Restore to specific LSN
cobaltdb-cli restore --from /backups/base.backup --target-lsn 1234567

# Restore to specific time
cobaltdb-cli restore --from /backups/base.backup --target-time "2024-01-01 12:00:00"
```

---

## Next Steps

### Learn More

- [API Reference](docs/API_REFERENCE.md)
- [SDK Guides](docs/SDK_GUIDES.md)
- [Performance Tuning](docs/PERFORMANCE_TUNING.md)
- [Security Guide](docs/SECURITY.md)
- [Operations Guide](docs/OPERATIONS.md)

### Examples

- [Basic CRUD](examples/basic_crud)
- [Transactions](examples/transactions)
- [Connection Pooling](examples/connection_pool)
- [Monitoring](examples/monitoring)

### Support

- GitHub Issues: https://github.com/cobaltdb/cobaltdb/issues
- Documentation: https://docs.cobaltdb.io
- Community: https://discord.gg/cobaltdb

---

## Quick Reference

### Common Commands

```bash
# Start server
cobaltdb-server --data-dir ./data

# Connect with CLI
cobaltdb-cli

# Run SQL file
cobaltdb-cli --file script.sql

# Check status
cobaltdb-cli ping

# Get stats
cobaltdb-cli stats

# Create backup
cobaltdb-cli backup create --output backup.db
```

### Connection Strings

```
# Simple
cobaltdb://localhost:4200/mydb

# With auth
cobaltdb://user:pass@localhost:4200/mydb

# With options
cobaltdb://localhost:4200/mydb?sslmode=require&timeout=30s
```

---

**Welcome to CobaltDB! 🚀**
