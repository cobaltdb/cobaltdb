# CobaltDB Technical Architecture

## Overview

CobaltDB is a layered database architecture implementing ACID transactions, MVCC, and SQL query processing in pure Go.

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 6: Network & Protocol                                      │
│ - MySQL wire protocol compatibility                             │
│ - Custom binary protocol                                        │
│ - TCP server with connection pooling                            │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ Layer 5: Engine API                                              │
│ - Database lifecycle (Open, Close)                              │
│ - Query execution (Exec, Query)                                 │
│ - Transaction management                                        │
│ - Connection pooling                                            │
│ - Metrics collection                                            │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4: Catalog & Schema Management                             │
│ - Table definitions (CREATE, ALTER, DROP)                       │
│ - Index management                                              │
│ - Views, Triggers, Procedures                                   │
│ - Foreign key constraints                                       │
│ - Statistics (ANALYZE)                                          │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3: Query Processing                                        │
│ - Lexer & Parser (SQL → AST)                                    │
│ - Query planner (basic optimization)                            │
│ - Expression evaluator                                          │
│ - JOIN execution (nested loop, hash join)                       │
│ - Aggregation (GROUP BY, HAVING)                                │
│ - Window functions                                              │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2: Transaction & Concurrency                               │
│ - Transaction manager (MVCC)                                    │
│ - Lock manager (RWMutex-based)                                  │
│ - WAL (Write-Ahead Log)                                         │
│ - Checkpointing                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: Storage Engine                                          │
│ - B+Tree indexes                                                │
│ - Buffer pool (LRU cache)                                       │
│ - Page manager (allocation/deallocation)                        │
│ - Disk backend (file/memory)                                    │
└─────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. Storage Layer (pkg/storage)

#### Buffer Pool
```go
type BufferPool struct {
    capacity   int                    // max pages in cache
    pages      map[uint32]*CachedPage // pageID -> cached page
    lru        *list.List             // LRU eviction list
    mu         sync.RWMutex
    backend    Backend
    wal        *WAL
    nextPageID uint32
}
```

- **Page Size**: 4096 bytes
- **Pin Count**: Atomic operations for thread-safe pinning
- **Eviction**: LRU with dirty page flushing

#### Page Manager
- **Free List**: Linked list of free pages on disk
- **Allocation**: Reuses free pages before allocating new
- **Metadata**: Stores root page ID, page count in meta page

#### WAL (Write-Ahead Log)
```go
type WAL struct {
    file       *os.File
    mu         sync.Mutex
    buffer     []WALRecord
    checkpointLSN uint64
}
```

- **Record Format**: `[Type:1][TxnID:8][DataLen:4][Data...]`
- **Durability**: Sync on commit (configurable)
- **Recovery**: Replays uncommitted transactions

### 2. B+Tree (pkg/btree)

#### In-Memory BTree
- **Hybrid Storage**: In-memory map + disk serialization
- **Memory Limit**: Configurable (default 64MB)
- **LRU Eviction**: Evicts least recently used entries

#### Disk BTree (DiskBTree)
- **Page Format**: `[Type:1][Flags:1][Count:2][Free:2][RightPtr:4][Entries...]`
- **Entry Format**: `[KeyLen:2][ValLen:2][Key...][Value...]`
- **Split Strategy**: When page > 50% full, split into two

### 3. Transaction Manager (pkg/txn)

#### MVCC Implementation
```go
type Transaction struct {
    ID        uint64
    State     TxnState
    Isolation IsolationLevel
    ReadSet   map[string]uint64  // key → version read
    WriteSet  map[string][]byte  // key → new value
    mu        sync.Mutex
}
```

- **Snapshot Isolation**: Default isolation level
- **Conflict Detection**: Write-write conflict detection
- **Versioning**: Monotonic transaction IDs

#### Lock Hierarchy
```
Catalog.mu (RWMutex)
  ├── tableTrees[table] → BTree.mu (RWMutex)
  │                         └── BufferPool.mu (RWMutex)
  └── indexTrees[index] → BTree.mu
```

**Rule**: Always acquire locks top-down, never upgrade RLock→Lock

### 4. Query Processing (pkg/query)

#### Lexer
- **Token Types**: 50+ SQL keywords, operators, literals
- **Buffered Reading**: Efficient multi-character lookahead
- **Position Tracking**: For error reporting

#### Parser
```
SQL → Tokens → AST (Abstract Syntax Tree)

Statement Types:
- SelectStmt: WITH, SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- InsertStmt: INSERT INTO, VALUES, ON CONFLICT
- UpdateStmt: UPDATE, SET, WHERE
- DeleteStmt: DELETE, WHERE
- CreateTableStmt: Columns, Constraints, Foreign Keys
- CreateIndexStmt: Index name, table, columns
```

#### Expression Evaluator
```go
type Expression interface {
    Type() ExprType
    String() string
}

// Supported Expressions:
// - LiteralExpr: numbers, strings, booleans, NULL
// - IdentifierExpr: column names
// - BinaryExpr: +, -, *, /, =, !=, <, >, AND, OR
// - UnaryExpr: NOT, -
// - FunctionCallExpr: COUNT, SUM, JSON_EXTRACT, etc.
// - CaseExpr: CASE WHEN...THEN...ELSE...END
// - WindowExpr: ROW_NUMBER(), RANK(), etc.
```

### 5. Catalog (pkg/catalog)

#### Schema Storage
```go
type Catalog struct {
    mu                sync.RWMutex
    tree              *btree.BTree        // Schema storage
    tables            map[string]*TableDef
    indexes           map[string]*IndexDef
    indexTrees        map[string]*btree.BTree
    tableTrees        map[string]*btree.BTree
    views             map[string]*query.SelectStmt
    triggers          map[string]*query.CreateTriggerStmt
    procedures        map[string]*query.CreateProcedureStmt
    materializedViews map[string]*MaterializedViewDef
    stats             map[string]*StatsTableStats
    undoLog           []undoEntry         // Transaction rollback
    savepoints        []savepointEntry    // Savepoint stack
}
```

#### Undo Log
For transaction rollback:
```go
type undoEntry struct {
    action       undoAction  // INSERT, UPDATE, DELETE, DDL
    tableName    string
    key          []byte
    oldValue     []byte
    indexChanges []indexUndoEntry
    // DDL specific fields...
}
```

### 6. Engine (pkg/engine)

#### Connection Management
```go
type DB struct {
    connSem         chan struct{}  // Connection limit semaphore
    activeConns     atomic.Int64   // Active connection count
    shutdownCh      chan struct{}  // Shutdown signal
    shutdownOnce    sync.Once      // Ensure single shutdown
}
```

#### Prepared Statement Cache
```go
type cachedStmt struct {
    stmt      query.Statement
    lastUsed  int64
    useCount  uint64
}

// LRU eviction when cache > 1000 statements
```

#### Panic Recovery
All public API methods have panic recovery:
```go
func (db *DB) Exec(...) (result Result, err error) {
    defer func() {
        if r := recover(); r != nil {
            err = fmt.Errorf("panic in Exec: %v\n%s", r, debug.Stack())
            // Log panic...
        }
    }()
    // ...
}
```

## Data Flow

### INSERT Flow
```
1. Engine.Exec()
   ↓
2. Parse SQL (or use cached statement)
   ↓
3. catalog.Insert()
   ↓
4. Acquire Catalog.mu.Lock()
   ↓
5. Validate constraints (NOT NULL, UNIQUE, CHECK)
   ↓
6. Check foreign key constraints
   ↓
7. Write to table BTree
   ↓
8. Update index BTrees
   ↓
9. Add to undoLog
   ↓
10. Release lock
   ↓
11. Return result
```

### SELECT Flow
```
1. Engine.Query()
   ↓
2. Parse SQL
   ↓
3. catalog.ExecuteCTE() / selectLocked()
   ↓
4. Acquire Catalog.mu.RLock()
   ↓
5. Build execution plan
   ↓
6. Scan table (index or full scan)
   ↓
7. Filter rows (WHERE)
   ↓
8. Apply JOINs
   ↓
9. Aggregate (GROUP BY)
   ↓
10. Window functions
   ↓
11. Sort (ORDER BY)
   ↓
12. Limit/Offset
   ↓
13. Release lock
   ↓
14. Return Rows iterator
```

### Transaction Flow
```
1. BEGIN
   - catalog.BeginTransaction(txnID)
   - txnActive = true
   - undoLog = []

2. INSERT/UPDATE/DELETE
   - Add undoEntry to undoLog
   - Modify data

3. COMMIT
   - Write WAL records
   - Clear undoLog
   - txnActive = false

4. ROLLBACK
   - Replay undoLog in reverse
   - Restore original values
   - Clear undoLog
   - txnActive = false
```

## Performance Characteristics

### Time Complexity

| Operation | Average | Worst Case |
|-----------|---------|------------|
| Point Lookup (indexed) | O(log n) | O(log n) |
| Point Lookup (no index) | O(n) | O(n) |
| Range Scan | O(log n + m) | O(log n + m) |
| Insert | O(log n) | O(log n) |
| Delete | O(log n) | O(log n) |
| Full Table Scan | O(n) | O(n) |
| JOIN (nested loop) | O(n × m) | O(n × m) |
| Aggregate | O(n) | O(n) |
| Sort | O(n log n) | O(n log n) |

*n = rows in table, m = rows in result*

### Space Complexity

| Structure | Overhead |
|-----------|----------|
| Row storage | ~20 bytes + JSON encoding |
| B+Tree index | ~8 bytes per entry |
| WAL | Append-only, truncated on checkpoint |
| Buffer pool | Configurable (default: 4MB) |

## Concurrency Model

### Locking Strategy
```
Reads:  Catalog.mu.RLock()
Writes: Catalog.mu.Lock()
```

### Single Writer Design
- Only one write transaction at a time
- Multiple read transactions can proceed concurrently
- Long-running SELECTs block writes (known limitation)

### Thread Safety
All components are thread-safe:
- BufferPool: RWMutex for cache, atomic for pin counts
- BTree: Mutex for structure, atomic for LRU
- Catalog: RWMutex for schema
- Transaction: Mutex for state

## Error Handling

### Error Categories
1. **Parse Errors**: Invalid SQL syntax
2. **Constraint Errors**: NOT NULL, UNIQUE, CHECK violations
3. **Foreign Key Errors**: Referential integrity violations
4. **Lock Errors**: Deadlock, timeout
5. **Storage Errors**: Disk full, I/O errors
6. **Internal Errors**: Panic recovery

### Recovery Strategies
- **Parse Error**: Return error, no state change
- **Constraint Error**: Rollback current statement only
- **FK Error**: Reject operation, no changes
- **Lock Error**: Retry or timeout
- **Storage Error**: Attempt rollback, may fail
- **Panic**: Recover, return error, database stays up

## Testing Strategy

### Test Coverage
- **Unit Tests**: Individual component testing
- **Integration Tests**: Cross-component workflows
- **E2E Tests**: Full SQL execution
- **Concurrency Tests**: Race detection
- **Benchmarks**: Performance regression

### Test Files
- 100+ test files across 21 packages
- 4500+ integration tests (v1-v84)
- E-commerce test suite (16 scenarios)

### Production Validation
```bash
# Run all tests
go test ./... -count=1 -timeout 300s

# Run benchmarks
go test -bench=. -benchtime=1s ./...

# Check for race conditions (requires CGO)
go test -race ./...
```

---

**Last Updated**: 2026-03-07
**Architecture Version**: v0.2.10
