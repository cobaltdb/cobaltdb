# API Reference

## Engine Package

### Opening a Database

```go
import "github.com/cobaltdb/cobaltdb/pkg/engine"

// In-memory database
db, err := engine.Open(":memory:", &engine.Options{
    InMemory:  true,
    CacheSize: 1024, // Buffer pool size in bytes
})

// Disk database
db, err := engine.Open("./mydb.db", &engine.Options{
    InMemory:  false,
    CacheSize: 1024 * 1024, // 1MB buffer pool
})
```

### Options

```go
type Options struct {
    InMemory  bool   // Use in-memory mode (default: false)
    CacheSize int    // Buffer pool size in bytes (default: 1024)
}
```

### Database Methods

#### Exec

Execute a SQL statement that doesn't return rows.

```go
result, err := db.Exec(ctx, "INSERT INTO users (name) VALUES (?)", "John")
// result.RowsAffected - number of affected rows
// result.LastInsertID - last insert ID if applicable
```

#### Query

Execute a SQL query and return rows.

```go
rows, err := db.Query(ctx, "SELECT name, email FROM users WHERE age > ?", 18)
defer rows.Close()

// Columns returns column names
columns := rows.Columns()

// Next advances to next row
for rows.Next() {
    var name, email string
    rows.Scan(&name, &email)
}
```

#### QueryRow

Execute a query and return a single row.

```go
row := db.QueryRow(ctx, "SELECT name FROM users WHERE id = ?", 1)
var name string
row.Scan(&name)
```

#### Begin

Start a new transaction.

```go
tx, err := db.Begin(ctx)
// use tx.Exec() or tx.Query()
// tx.Commit() or tx.Rollback()
```

### Transaction Methods

```go
tx, _ := db.Begin(ctx)

tx.Exec(ctx, "INSERT INTO users (name) VALUES (?)", "John")
tx.Commit() // or tx.Rollback()
```

## Server Package

### Starting a Server

```go
import (
    "github.com/cobaltdb/cobaltdb/pkg/engine"
    "github.com/cobaltdb/cobaltdb/pkg/server"
)

// Open database
db, _ := engine.Open(":memory:", &engine.Options{InMemory: true})

// Create server
srv, _ := server.New(db, &server.Config{
    Address: ":4200",
})

// Start listening
err := srv.Listen(":4200")
```

### Connecting via Wire Protocol

The server uses a simple binary protocol:

1. Send message length (4 bytes, little-endian)
2. Send message type (1 byte)
3. Send MessagePack-encoded payload

#### Message Types

| Type | Value | Description |
|------|-------|-------------|
| MsgQuery | 0x01 | SQL query |
| MsgResult | 0x10 | Query result |
| MsgOK | 0x11 | Execution success |
| MsgError | 0x12 | Error response |
| MsgPing | 0x20 | Ping |
| MsgPong | 0x21 | Pong |

#### Query Message

```go
type QueryMessage struct {
    SQL    string        `msgpack:"sql"`
    Params []interface{} `msgpack:"params,omitempty"`
}
```

#### Result Message

```go
type ResultMessage struct {
    Columns []string         `msgpack:"cols"`
    Types   []string         `msgpack:"types"`
    Rows    [][]interface{}  `msgpack:"rows"`
    Count   int64            `msgpack:"count"`
}
```

## Catalog Package (Low-Level)

### Creating a Catalog

```go
import (
    "github.com/cobaltdb/cobaltdb/pkg/catalog"
    "github.com/cobaltdb/cobaltdb/pkg/storage"
)

backend := storage.NewMemory()
pool := storage.NewBufferPool(1024, backend)
cat := catalog.New(nil, pool)
```

### Creating a Table

```go
stmt := &query.CreateTableStmt{
    Table: "users",
    Columns: []*query.ColumnDef{
        {Name: "id", Type: query.TokenInteger, PrimaryKey: true},
        {Name: "name", Type: query.TokenText, NotNull: true},
    },
}
cat.CreateTable(stmt)
```

## Query Package

### Parsing SQL

```go
import "github.com/cobaltdb/cobaltdb/pkg/query"

stmt, err := query.Parse("SELECT * FROM users WHERE age > ?")
```

### Expression Evaluation

```go
import "github.com/cobaltdb/cobaltdb/pkg/catalog"

// Evaluate an expression
value, err := catalog.EvalExpression(expr, []interface{}{args...})
```

## Storage Package

### Buffer Pool

```go
backend := storage.NewDisk("./data")
pool := storage.NewBufferPool(1024*1024, backend) // 1MB
```

### Memory Backend

```go
backend := storage.NewMemory()
```
