# CobaltDB — Embedded Database Engine Architecture

> A lightweight, embeddable database engine written in Go with SQL + JSON query support,
> persistent storage, in-memory mode, and multi-language SDKs.

---

## 1. Vizyon & Positioning

```
SQLite        → Single-file relational, C, 40+ yıllık legacy
BoltDB        → Key-value only, no query language
BadgerDB      → LSM-based KV, no SQL
DuckDB        → Analytical (OLAP), embedded
─────────────────────────────────────────────────
CobaltDB         → Hybrid document-relational, Go-native,
                SQL + JSONPath queries, embed veya standalone
```

**Hedef:** SQLite'ın basitliği + MongoDB'nin document flexibility'si + modern Go ecosystem uyumu.

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      CLIENT LAYER                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────┐  │
│  │  Go SDK  │  │  TS SDK  │  │ Python   │  │  REST/gRPC │  │
│  │ (embed)  │  │ (TCP)    │  │ SDK(TCP) │  │  HTTP API  │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └─────┬──────┘  │
│       │              │             │               │         │
│  ┌────▼──────────────▼─────────────▼───────────────▼──────┐  │
│  │              WIRE PROTOCOL (MessagePack/TCP)            │  │
│  └────────────────────────┬───────────────────────────────┘  │
└───────────────────────────┼──────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────┐
│                      SERVER CORE                             │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐ │
│  │   SQL/Query  │  │   Query      │  │   Query             │ │
│  │   Parser     │──▶  Planner &   │──▶  Executor           │ │
│  │   (Lexer+AST)│  │  Optimizer   │  │  (Iterator Model)   │ │
│  └─────────────┘  └──────────────┘  └──────────┬──────────┘ │
│                                                  │           │
│  ┌──────────────────────────────────────────────▼─────────┐ │
│  │                TRANSACTION MANAGER                      │ │
│  │         (MVCC — Snapshot Isolation)                     │ │
│  └──────────────────────────┬─────────────────────────────┘ │
│                              │                               │
│  ┌──────────────────────────▼─────────────────────────────┐ │
│  │                 STORAGE ENGINE                          │ │
│  │  ┌──────────┐  ┌──────────┐  ┌───────────────────────┐ │ │
│  │  │  B+Tree   │  │  Index   │  │   Buffer Pool         │ │ │
│  │  │  (Pages)  │  │  Manager │  │   (Page Cache)        │ │ │
│  │  └─────┬────┘  └──────────┘  └───────────┬───────────┘ │ │
│  │        │                                   │             │ │
│  │  ┌─────▼───────────────────────────────────▼───────────┐ │ │
│  │  │              PAGE MANAGER                           │ │ │
│  │  │  ┌────────────┐  ┌─────────────┐  ┌─────────────┐  │ │ │
│  │  │  │   Pager     │  │   WAL       │  │  Free Page  │  │ │ │
│  │  │  │  (Read/Write│  │  (Write-    │  │  List       │  │ │ │
│  │  │  │   Pages)    │  │   Ahead Log)│  │             │  │ │ │
│  │  │  └──────┬─────┘  └──────┬──────┘  └─────────────┘  │ │ │
│  │  └─────────┼───────────────┼───────────────────────────┘ │ │
│  └────────────┼───────────────┼─────────────────────────────┘ │
│               │               │                               │
│  ┌────────────▼───────────────▼─────────────────────────────┐ │
│  │              I/O LAYER                                    │ │
│  │  ┌──────────────────┐  ┌───────────────────────────────┐ │ │
│  │  │  Disk Backend    │  │  Memory Backend               │ │ │
│  │  │  (mmap or pread) │  │  ([]byte slices)              │ │ │
│  │  └──────────────────┘  └───────────────────────────────┘ │ │
│  └──────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

---

## 3. Core Components — Detaylı Tasarım

### 3.1 Storage Backend (I/O Layer)

İki mode destekle: **Disk** ve **Memory**.

```go
// backend.go — Storage backend interface
type Backend interface {
    ReadAt(buf []byte, offset int64) (int, error)
    WriteAt(buf []byte, offset int64) (int, error)
    Sync() error
    Size() int64
    Truncate(size int64) error
    Close() error
}

// Disk backend — uses pread/pwrite (mmap optional)
type DiskBackend struct {
    file     *os.File
    filePath string
    fileSize int64
    mu       sync.RWMutex
}

// Memory backend — RAM only, optional snapshot-to-disk
type MemoryBackend struct {
    data []byte
    mu   sync.RWMutex
}
```

**Neden mmap yerine pread/pwrite?**
- mmap, Go'nun GC'si ile sorunlu olabiliyor (page fault overhead)
- pread/pwrite daha predictable, buffer pool ile kontrol sende
- İstenirse mmap backend de eklenebilir (interface sayesinde swap edilebilir)

### 3.2 Page Manager

Her şey **page** bazlı. SQLite gibi fixed-size pages.

```go
const (
    PageSize     = 4096  // 4KB default, configurable (4K/8K/16K/32K)
    MaxPageSize  = 65536 // 64KB max
)

// Page types
const (
    PageTypeMeta     uint8 = 0x01  // Database metadata (page 0)
    PageTypeInternal uint8 = 0x02  // B+Tree internal node
    PageTypeLeaf     uint8 = 0x03  // B+Tree leaf node
    PageTypeOverflow uint8 = 0x04  // Large value overflow
    PageTypeFreeList uint8 = 0x05  // Free page tracking
)

// Page header (her page'in ilk 16 byte'ı)
type PageHeader struct {
    PageID    uint32  // 4 bytes — page number
    PageType  uint8   // 1 byte  — type flag
    Flags     uint8   // 1 byte  — dirty, pinned, etc.
    CellCount uint16  // 2 bytes — number of cells in page
    FreeStart uint16  // 2 bytes — offset to free space start
    FreeEnd   uint16  // 2 bytes — offset to free space end
    RightPtr  uint32  // 4 bytes — right sibling / overflow pointer
}                     // Total: 16 bytes

// Meta page (page 0 — database header)
type MetaPage struct {
    Magic       [4]byte  // "OXDB"
    Version     uint32   // format version
    PageSize    uint32   // page size in bytes
    PageCount   uint32   // total pages in file
    FreeListID  uint32   // page ID of free list head
    RootPageID  uint32   // root page of system catalog B+Tree
    TxnCounter  uint64   // monotonic transaction counter
    Checksum    uint32   // CRC32 of this page
}
```

### 3.3 Buffer Pool (Page Cache)

Disk'ten okunan page'leri RAM'de cache'le. LRU eviction.

```go
type BufferPool struct {
    capacity int                    // max pages in cache
    pages    map[uint32]*CachedPage // pageID → cached page
    lru      *list.List             // LRU eviction list
    mu       sync.RWMutex
    backend  Backend
    wal      *WAL
}

type CachedPage struct {
    id      uint32
    data    []byte    // PageSize bytes
    dirty   bool      // modified since last flush?
    pinned  int32     // pin count (atomic)
    lruElem *list.Element
}

func (bp *BufferPool) GetPage(pageID uint32) (*CachedPage, error) {
    bp.mu.RLock()
    if p, ok := bp.pages[pageID]; ok {
        bp.mu.RUnlock()
        bp.touchLRU(p)
        atomic.AddInt32(&p.pinned, 1)
        return p, nil
    }
    bp.mu.RUnlock()

    // Cache miss — read from disk
    bp.mu.Lock()
    defer bp.mu.Unlock()

    // Double-check after lock upgrade
    if p, ok := bp.pages[pageID]; ok {
        bp.touchLRU(p)
        atomic.AddInt32(&p.pinned, 1)
        return p, nil
    }

    // Evict if at capacity
    if len(bp.pages) >= bp.capacity {
        bp.evict()
    }

    data := make([]byte, PageSize)
    _, err := bp.backend.ReadAt(data, int64(pageID)*int64(PageSize))
    if err != nil {
        return nil, err
    }

    page := &CachedPage{id: pageID, data: data, pinned: 1}
    bp.pages[pageID] = page
    page.lruElem = bp.lru.PushFront(page)
    return page, nil
}
```

### 3.4 Write-Ahead Log (WAL)

Crash recovery için. Her write önce WAL'a gider.

```go
// WAL record format:
// [TxnID:8][Type:1][PageID:4][Offset:2][Length:2][Data:N][CRC:4]

type WALRecordType uint8

const (
    WALInsert   WALRecordType = 0x01
    WALUpdate   WALRecordType = 0x02
    WALDelete   WALRecordType = 0x03
    WALCommit   WALRecordType = 0x04
    WALRollback WALRecordType = 0x05
    WALCheckpoint WALRecordType = 0x06
)

type WAL struct {
    file       *os.File
    mu         sync.Mutex
    bufWriter  *bufio.Writer
    lsn        uint64  // Log Sequence Number (monotonic)
    checkpoint uint64  // last checkpoint LSN
}

type WALRecord struct {
    LSN    uint64
    TxnID  uint64
    Type   WALRecordType
    PageID uint32
    Offset uint16
    Data   []byte
}

func (w *WAL) Append(record *WALRecord) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    w.lsn++
    record.LSN = w.lsn

    // Encode record
    buf := encodeWALRecord(record)

    // Write + CRC
    crc := crc32.ChecksumIEEE(buf)
    if _, err := w.bufWriter.Write(buf); err != nil {
        return err
    }
    if err := binary.Write(w.bufWriter, binary.LittleEndian, crc); err != nil {
        return err
    }

    // Commit records must be fsynced immediately
    if record.Type == WALCommit {
        w.bufWriter.Flush()
        return w.file.Sync()
    }
    return nil
}

// Checkpoint — flush dirty pages to main DB file, truncate WAL
func (w *WAL) Checkpoint(bp *BufferPool) error {
    // 1. Flush all dirty pages from buffer pool to disk
    // 2. Update checkpoint LSN
    // 3. Truncate WAL file
    // 4. Fsync main DB file
    return nil
}

// Recovery — replay WAL after crash
func (w *WAL) Recover(bp *BufferPool) error {
    // 1. Read all records after last checkpoint
    // 2. Redo committed transactions
    // 3. Undo uncommitted transactions
    return nil
}
```

### 3.5 B+Tree

Ana veri yapısı. Key-Value pairs, sorted, range query desteği.

```go
// B+Tree — on-disk, page-based
type BTree struct {
    rootPageID uint32
    pool       *BufferPool
    order      int  // max keys per node (derived from page size)
}

// Cell — bir key-value pair (leaf node'da)
type Cell struct {
    KeySize   uint16
    ValueSize uint32
    Key       []byte
    Value     []byte  // document data (JSON/MessagePack encoded)
}

// Internal node cell — key + child page pointer
type InternalCell struct {
    KeySize    uint16
    Key        []byte
    ChildPageID uint32
}

// Leaf page layout:
// [PageHeader:16][PrevLeaf:4][NextLeaf:4]
// [CellOffsets: 2*CellCount bytes]  — slot array pointing to cells
// [... free space ...]
// [Cell N][...][Cell 1][Cell 0]  — cells grow from end of page

// Core operations
func (bt *BTree) Get(key []byte) ([]byte, error)
func (bt *BTree) Put(key []byte, value []byte) error
func (bt *BTree) Delete(key []byte) error
func (bt *BTree) Scan(startKey, endKey []byte) *Iterator

// Iterator for range scans
type Iterator struct {
    tree     *BTree
    currentPage uint32
    currentIdx  int
    endKey      []byte
}

func (it *Iterator) Next() (key, value []byte, err error)
func (it *Iterator) Valid() bool
func (it *Iterator) Close()
```

**B+Tree Split/Merge Stratejisi:**
- Leaf node dolduğunda → split into two, promote middle key to parent
- Internal node dolduğunda → split, promote middle key up
- Leaf node %25 altına düştüğünde → merge with sibling or redistribute
- Bu SQLite'ın kullandığı strateji ile aynı

### 3.6 Index Manager

Primary index (B+Tree on primary key) + Secondary indexes.

```go
type IndexType uint8

const (
    IndexTypeBTree   IndexType = 0x01  // Default — sorted, range queries
    IndexTypeHash    IndexType = 0x02  // Exact match only, O(1) lookup
    IndexTypeJSON    IndexType = 0x03  // JSON path index
)

type IndexDef struct {
    Name       string
    TableName  string
    Columns    []string    // ["name", "age"] or ["data.address.city"] for JSON
    Type       IndexType
    Unique     bool
    Expression string      // computed index expression (optional)
}

type IndexManager struct {
    indexes map[string]*BTree  // indexName → B+Tree
    catalog *SystemCatalog
    pool    *BufferPool
}

// JSON path index — extract value from JSON doc, index it
// CREATE INDEX idx_city ON users (data->>'address.city')
// Bu, JSON doc içindeki nested field'ı çıkarıp B+Tree'ye koyar
```

### 3.7 Transaction Manager (MVCC)

```go
type IsolationLevel uint8

const (
    ReadCommitted    IsolationLevel = 0x01
    SnapshotIsolation IsolationLevel = 0x02  // Default
    Serializable     IsolationLevel = 0x03
)

type TxnState uint8

const (
    TxnActive    TxnState = 0x01
    TxnCommitted TxnState = 0x02
    TxnAborted   TxnState = 0x03
)

type Transaction struct {
    ID         uint64
    State      TxnState
    Isolation  IsolationLevel
    StartTS    uint64          // snapshot timestamp
    ReadSet    map[string]uint64  // key → version read
    WriteSet   map[string][]byte  // key → new value (buffered writes)
    mu         sync.Mutex
}

type TxnManager struct {
    counter    uint64              // atomic, monotonic
    active     map[uint64]*Transaction
    mu         sync.RWMutex
    wal        *WAL
    pool       *BufferPool
}

func (tm *TxnManager) Begin(iso IsolationLevel) *Transaction {
    id := atomic.AddUint64(&tm.counter, 1)
    txn := &Transaction{
        ID:        id,
        State:     TxnActive,
        Isolation: iso,
        StartTS:   id,
        ReadSet:   make(map[string]uint64),
        WriteSet:  make(map[string][]byte),
    }
    tm.mu.Lock()
    tm.active[id] = txn
    tm.mu.Unlock()
    return txn
}

func (tm *TxnManager) Commit(txn *Transaction) error {
    txn.mu.Lock()
    defer txn.mu.Unlock()

    // 1. Conflict detection (for Snapshot Isolation)
    if txn.Isolation >= SnapshotIsolation {
        if err := tm.detectConflicts(txn); err != nil {
            tm.Rollback(txn)
            return err // ErrConflict — client should retry
        }
    }

    // 2. Write WAL commit record
    for key, value := range txn.WriteSet {
        tm.wal.Append(&WALRecord{
            TxnID: txn.ID,
            Type:  WALUpdate,
            Data:  encodeKV(key, value),
        })
    }
    tm.wal.Append(&WALRecord{TxnID: txn.ID, Type: WALCommit})

    // 3. Apply writes to B+Tree (through buffer pool)
    for key, value := range txn.WriteSet {
        // apply to storage
    }

    txn.State = TxnCommitted
    tm.removeActive(txn.ID)
    return nil
}
```

**MVCC Yaklaşımı:**
- Her record'a version stamp (txn ID) ekle
- Read sırasında, txn'ın start timestamp'inden önceki en son committed version'ı oku
- Write sırasında, write set'e buffer'la, commit'te conflict check yap
- Garbage collection: eski version'ları periyodik temizle

---

## 4. Query Engine

### 4.1 SQL + JSON Query Language

```sql
-- Klasik SQL
CREATE TABLE users (
    id    INTEGER PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT UNIQUE,
    data  JSON           -- First-class JSON column
);

INSERT INTO users (id, name, email, data) VALUES (
    1, 'Ersin', 'ersin@eco.dev',
    '{"role": "CTO", "skills": ["Go", "TypeScript"], "address": {"city": "Tallinn"}}'
);

-- JSON query — arrow operator
SELECT name, data->>'role' AS role
FROM users
WHERE data->>'address.city' = 'Tallinn';

-- JSON array contains
SELECT * FROM users
WHERE data->'skills' @> '["Go"]';

-- Nested JSON update
UPDATE users SET data = json_set(data, 'role', '"CEO"') WHERE id = 1;

-- JSON path index
CREATE INDEX idx_user_city ON users (data->>'address.city');

-- Document mode — schema-free collection
CREATE COLLECTION logs;
INSERT INTO logs VALUES ('{"level":"info","msg":"started","ts":1234567890}');
SELECT * FROM logs WHERE doc->>'level' = 'error' AND doc->>'ts' > 1234567800;
```

### 4.2 Lexer & Parser

```go
// Token types
type TokenType int

const (
    // Keywords
    TK_SELECT TokenType = iota
    TK_INSERT
    TK_UPDATE
    TK_DELETE
    TK_CREATE
    TK_TABLE
    TK_COLLECTION
    TK_INDEX
    TK_FROM
    TK_WHERE
    TK_AND
    TK_OR
    TK_NOT
    TK_ORDER
    TK_BY
    TK_LIMIT
    TK_OFFSET
    TK_JOIN
    TK_ON
    TK_GROUP
    TK_HAVING
    TK_AS
    TK_SET
    TK_VALUES
    TK_INTO
    TK_PRIMARY
    TK_KEY
    TK_UNIQUE
    TK_JSON

    // Operators
    TK_ARROW       // ->  (JSON extract, keeps type)
    TK_ARROW2      // ->> (JSON extract as text)
    TK_CONTAINS    // @>  (JSON contains)
    TK_EQ          // =
    TK_NEQ         // !=
    TK_LT          // <
    TK_GT          // >
    TK_LTE         // <=
    TK_GTE         // >=

    // Literals & identifiers
    TK_IDENT
    TK_STRING
    TK_NUMBER
    TK_BLOB
    TK_NULL
    TK_TRUE
    TK_FALSE

    // Punctuation
    TK_LPAREN
    TK_RPAREN
    TK_COMMA
    TK_SEMICOLON
    TK_DOT
    TK_STAR
)

// AST Nodes
type Node interface{ nodeType() string }

type SelectStmt struct {
    Columns  []Expression    // SELECT ...
    From     *TableRef       // FROM ...
    Joins    []*JoinClause   // JOIN ...
    Where    Expression      // WHERE ...
    GroupBy  []Expression    // GROUP BY ...
    Having   Expression      // HAVING ...
    OrderBy  []*OrderByExpr  // ORDER BY ...
    Limit    *int64
    Offset   *int64
}

type Expression interface{ exprType() string }

type BinaryExpr struct {
    Left  Expression
    Op    TokenType
    Right Expression
}

type JSONPathExpr struct {
    Column string
    Path   string      // "address.city"
    AsText bool        // ->> vs ->
}

type FunctionCall struct {
    Name string
    Args []Expression
}

// Parser
type Parser struct {
    lexer  *Lexer
    tokens []Token
    pos    int
}

func (p *Parser) Parse() (Node, error) {
    tok := p.peek()
    switch tok.Type {
    case TK_SELECT:
        return p.parseSelect()
    case TK_INSERT:
        return p.parseInsert()
    case TK_UPDATE:
        return p.parseUpdate()
    case TK_DELETE:
        return p.parseDelete()
    case TK_CREATE:
        return p.parseCreate()
    default:
        return nil, fmt.Errorf("unexpected token: %s", tok.Literal)
    }
}
```

### 4.3 Query Planner & Optimizer

```go
// Logical plan nodes
type PlanNode interface {
    Schema() []Column
    String() string
}

type SeqScan struct {
    Table string
    Filter Expression
}

type IndexScan struct {
    Table    string
    Index    string
    StartKey []byte
    EndKey   []byte
    Filter   Expression
}

type NestedLoopJoin struct {
    Left      PlanNode
    Right     PlanNode
    Condition Expression
}

type HashJoin struct {
    Left      PlanNode
    Right     PlanNode
    LeftKey   Expression
    RightKey  Expression
}

type Sort struct {
    Input PlanNode
    Keys  []*OrderByExpr
}

type Limit struct {
    Input  PlanNode
    Count  int64
    Offset int64
}

type Projection struct {
    Input   PlanNode
    Columns []Expression
}

// Query optimizer — rule-based (cost-based fazla karmaşık olur başlangıç için)
type Optimizer struct {
    catalog *SystemCatalog
}

func (o *Optimizer) Optimize(plan PlanNode) PlanNode {
    plan = o.pushDownFilters(plan)      // WHERE clause'ı scan'a yaklaştır
    plan = o.useIndexes(plan)           // SeqScan → IndexScan dönüşümü
    plan = o.eliminateRedundant(plan)   // gereksiz projection'ları kaldır
    return plan
}

// Filter pushdown:  Projection → Filter → SeqScan
// becomes:          Projection → IndexScan(with filter)
```

### 4.4 Query Executor (Volcano/Iterator Model)

```go
// Her plan node bir iterator olur
type Executor interface {
    Init() error
    Next() (*Row, error)  // nil = done
    Close() error
}

type SeqScanExecutor struct {
    table   *Table
    iter    *BTree.Iterator
    filter  Expression
    txn     *Transaction
}

func (e *SeqScanExecutor) Next() (*Row, error) {
    for {
        key, val, err := e.iter.Next()
        if err != nil || !e.iter.Valid() {
            return nil, err
        }
        row := decodeRow(key, val)

        // Apply filter
        if e.filter != nil {
            match, err := evalExpr(e.filter, row, e.txn)
            if err != nil { return nil, err }
            if !match { continue }
        }
        return row, nil
    }
}

// JSON expression evaluator
func evalJSONPath(doc []byte, path string) (interface{}, error) {
    // "address.city" → parse JSON, navigate path
    // Fast path: use jsoniter or custom parser for zero-alloc extraction
    parts := strings.Split(path, ".")
    var current interface{}
    json.Unmarshal(doc, &current)
    for _, part := range parts {
        m, ok := current.(map[string]interface{})
        if !ok { return nil, ErrInvalidPath }
        current = m[part]
    }
    return current, nil
}
```

---

## 5. System Catalog

Database metadata — tables, indexes, columns — hepsi de B+Tree'de saklanır.

```go
type SystemCatalog struct {
    tree *BTree  // system catalog B+Tree
}

// Catalog entries (stored as JSON in catalog B+Tree)
type TableDef struct {
    Name       string      `json:"name"`
    Type       string      `json:"type"`  // "table" or "collection"
    Columns    []ColumnDef `json:"columns"`
    PrimaryKey string      `json:"primary_key"`
    CreatedAt  int64       `json:"created_at"`
}

type ColumnDef struct {
    Name       string `json:"name"`
    Type       string `json:"type"`    // INTEGER, TEXT, REAL, BLOB, JSON, BOOLEAN
    NotNull    bool   `json:"not_null"`
    Unique     bool   `json:"unique"`
    Default    string `json:"default,omitempty"`
}

// Key scheme for catalog:
// "tbl:{name}"     → TableDef JSON
// "idx:{name}"     → IndexDef JSON
// "seq:{name}"     → sequence/autoincrement counter
```

---

## 6. JSON Document Engine

First-class JSON support — hem column type olarak hem de schema-free collection.

```go
// Fast JSON operations — zero-copy where possible
type JSONEngine struct{}

// JSON functions available in SQL
// json_extract(doc, '$.address.city')   → value
// json_set(doc, '$.role', '"CEO"')      → updated doc
// json_remove(doc, '$.temporary')       → doc without key
// json_array_length(doc, '$.skills')    → int
// json_each(doc, '$.skills')            → table-valued function
// json_valid(text)                      → boolean
// json_type(doc, '$.field')             → "string"/"number"/"object"/etc.

// Internal JSON storage: MessagePack for compactness
// JSON string → parse → MessagePack bytes → store in B+Tree
// Query time: MessagePack → navigate path → extract value
// Bu, raw JSON text'ten %30-50 daha compact ve daha hızlı parse edilir

type JSONValue struct {
    raw []byte // MessagePack encoded
}

func (j *JSONValue) Get(path string) (*JSONValue, error)
func (j *JSONValue) Set(path string, value interface{}) (*JSONValue, error)
func (j *JSONValue) Delete(path string) (*JSONValue, error)
func (j *JSONValue) AsString() (string, error)
func (j *JSONValue) AsInt() (int64, error)
func (j *JSONValue) AsFloat() (float64, error)
func (j *JSONValue) AsArray() ([]*JSONValue, error)
func (j *JSONValue) ToJSON() ([]byte, error)  // → JSON string
```

---

## 7. Wire Protocol & Server Mode

Embedded mode (Go library) + Standalone server mode.

```go
// Server mode — TCP + MessagePack protocol
type Server struct {
    listener net.Listener
    db       *Database
    clients  map[uint64]*ClientConn
}

// Protocol: simple request/response over TCP
// [MsgLength:4][MsgType:1][Payload:N]

type MsgType uint8

const (
    MsgQuery     MsgType = 0x01  // SQL query string
    MsgPrepare   MsgType = 0x02  // Prepared statement
    MsgExecute   MsgType = 0x03  // Execute prepared
    MsgResult    MsgType = 0x10  // Query result rows
    MsgOK        MsgType = 0x11  // Execution success
    MsgError     MsgType = 0x12  // Error response
    MsgPing      MsgType = 0x20
    MsgPong      MsgType = 0x21
)

// Result encoding — column-oriented for efficiency
type ResultSet struct {
    Columns []string         `msgpack:"cols"`
    Types   []string         `msgpack:"types"`
    Rows    [][]interface{}  `msgpack:"rows"`
    Count   int64            `msgpack:"count"`
}
```

---

## 8. Public API (Go SDK — Embedded Mode)

```go
package cobaltdb

import "context"

// Open or create a database
func Open(path string, opts ...Option) (*DB, error)

// Options
func WithPageSize(size int) Option
func WithCacheSize(pages int) Option
func WithInMemory() Option              // RAM-only mode
func WithWAL(enabled bool) Option
func WithJournal(mode JournalMode) Option
func WithSyncMode(mode SyncMode) Option

// DB — main database handle
type DB struct { /* ... */ }

func (db *DB) Close() error

// Exec — execute statement, no result rows
func (db *DB) Exec(ctx context.Context, sql string, args ...interface{}) (Result, error)

// Query — execute query, return rows
func (db *DB) Query(ctx context.Context, sql string, args ...interface{}) (*Rows, error)

// QueryRow — single row
func (db *DB) QueryRow(ctx context.Context, sql string, args ...interface{}) *Row

// Transactions
func (db *DB) Begin(ctx context.Context) (*Tx, error)
func (db *DB) BeginWith(ctx context.Context, opts TxOptions) (*Tx, error)

type Tx struct { /* ... */ }
func (tx *Tx) Exec(ctx context.Context, sql string, args ...interface{}) (Result, error)
func (tx *Tx) Query(ctx context.Context, sql string, args ...interface{}) (*Rows, error)
func (tx *Tx) Commit() error
func (tx *Tx) Rollback() error

// Prepared statements
func (db *DB) Prepare(ctx context.Context, sql string) (*Stmt, error)
type Stmt struct { /* ... */ }
func (s *Stmt) Exec(ctx context.Context, args ...interface{}) (Result, error)
func (s *Stmt) Query(ctx context.Context, args ...interface{}) (*Rows, error)
func (s *Stmt) Close() error

// Collection API — document/JSON mode
func (db *DB) Collection(name string) *Collection

type Collection struct { /* ... */ }
func (c *Collection) Insert(ctx context.Context, doc interface{}) (string, error)
func (c *Collection) FindOne(ctx context.Context, filter string) (*Document, error)
func (c *Collection) Find(ctx context.Context, filter string) (*Cursor, error)
func (c *Collection) Update(ctx context.Context, filter string, update interface{}) (int64, error)
func (c *Collection) Delete(ctx context.Context, filter string) (int64, error)

// Rows iteration
type Rows struct { /* ... */ }
func (r *Rows) Next() bool
func (r *Rows) Scan(dest ...interface{}) error
func (r *Rows) Columns() []string
func (r *Rows) Close() error

// Convenience: Scan into struct
func (r *Row) ScanStruct(dest interface{}) error

// ─────────────────────────────────────────────
// USAGE EXAMPLE
// ─────────────────────────────────────────────

func main() {
    // Persistent database
    db, _ := cobaltdb.Open("./myapp.cobalt",
        cobaltdb.WithCacheSize(1024),  // 1024 pages = 4MB cache
        cobaltdb.WithWAL(true),
    )
    defer db.Close()

    // Create table with JSON column
    db.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS users (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            name  TEXT NOT NULL,
            email TEXT UNIQUE,
            meta  JSON
        )
    `)

    // Insert with JSON
    db.Exec(ctx, `INSERT INTO users (name, email, meta) VALUES (?, ?, ?)`,
        "Ersin", "ersin@eco.dev",
        `{"role":"CTO","skills":["Go","TypeScript"],"loc":"Tallinn"}`,
    )

    // Query with JSON path
    rows, _ := db.Query(ctx,
        `SELECT name, meta->>'role', meta->>'loc' FROM users WHERE meta->>'loc' = ?`,
        "Tallinn",
    )
    for rows.Next() {
        var name, role, loc string
        rows.Scan(&name, &role, &loc)
        fmt.Printf("%s (%s) — %s\n", name, role, loc)
    }

    // Transaction
    tx, _ := db.Begin(ctx)
    tx.Exec(ctx, `UPDATE users SET meta = json_set(meta, 'role', '"CEO"') WHERE id = ?`, 1)
    tx.Commit()

    // In-memory database
    memDB, _ := cobaltdb.Open(":memory:")
    defer memDB.Close()

    // Collection mode (MongoDB-like)
    logs := db.Collection("logs")
    logs.Insert(ctx, map[string]interface{}{
        "level": "info",
        "msg":   "server started",
        "ts":    time.Now().Unix(),
    })

    cursor, _ := logs.Find(ctx, `doc->>'level' = 'error'`)
    for cursor.Next() {
        var doc map[string]interface{}
        cursor.Decode(&doc)
        fmt.Println(doc)
    }
}
```

---

## 9. SDK'lar (Remote Mode)

### 9.1 TypeScript/Node.js SDK

```typescript
import { CobaltDB } from '@cobaltg/cobaltdb';

// Connect to server
const db = new CobaltDB('tcp://localhost:4200');

// Or embedded via NAPI addon
const db = new CobaltDB('./myapp.cobalt');

// Query
const users = await db.query<User>(
  `SELECT * FROM users WHERE meta->>'role' = $1`,
  ['CTO']
);

// Collection mode
const logs = db.collection('logs');
await logs.insert({ level: 'info', msg: 'hello' });
const errors = await logs.find({ 'level': 'error' });

// Transactions
const tx = await db.begin();
await tx.exec(`UPDATE users SET name = $1 WHERE id = $2`, ['Ersin', 1]);
await tx.commit();
```

### 9.2 Python SDK

```python
import cobaltdb

# Connect
db = cobaltdb.connect("tcp://localhost:4200")

# Or embedded via ctypes/cffi
db = cobaltdb.open("./myapp.cobalt")

# Query
rows = db.query("SELECT * FROM users WHERE meta->>'loc' = ?", ("Tallinn",))
for row in rows:
    print(row["name"], row["meta"])

# Collection
logs = db.collection("logs")
logs.insert({"level": "info", "msg": "started"})
```

### 9.3 REST/HTTP API

```
POST /query
Content-Type: application/json

{
  "sql": "SELECT * FROM users WHERE meta->>'role' = ?",
  "params": ["CTO"]
}

→ 200 OK
{
  "columns": ["id", "name", "email", "meta"],
  "rows": [
    [1, "Ersin", "ersin@eco.dev", {"role":"CTO","skills":["Go","TS"]}]
  ],
  "count": 1,
  "duration_ms": 0.45
}
```

---

## 10. File Format

```
┌─────────────────────────────────────────┐
│  Page 0: Meta Page (Database Header)    │  ← "OXDB" magic, version, config
├─────────────────────────────────────────┤
│  Page 1: Free List Page                 │  ← tracks deallocated pages
├─────────────────────────────────────────┤
│  Page 2: System Catalog Root            │  ← B+Tree root for table/index defs
├─────────────────────────────────────────┤
│  Page 3+: Data Pages                    │  ← B+Tree nodes (internal + leaf)
│  ...                                    │
│  Page N: Overflow Pages                 │  ← large values that don't fit
└─────────────────────────────────────────┘

Separate file: myapp.cobalt.wal  ← Write-Ahead Log
```

**File Extension:** `.cobalt`

---

## 11. Benchmarks Hedefi

```
Target Performance (single-threaded, SSD):

Point lookup (by PK):           < 5μs  (in-cache), < 50μs (disk)
JSON path query (indexed):      < 10μs (in-cache)
Sequential scan (100K rows):    < 50ms
Insert (single row):            < 10μs (WAL mode)
Bulk insert (100K rows):        < 500ms
Transaction commit:             < 100μs
Memory footprint:               ~10MB base + cache

Comparison targets:
SQLite point lookup:            ~2μs
BoltDB point lookup:            ~5μs
```

---

## 12. Proje Yapısı

```
cobaltdb/
├── cmd/
│   ├── cobaltdb-server/          # Standalone server binary
│   │   └── main.go
│   └── cobaltdb-cli/             # CLI client (REPL)
│       └── main.go
│
├── pkg/
│   ├── engine/                # Core database engine
│   │   ├── database.go        # DB struct, Open(), Close()
│   │   ├── options.go         # Configuration
│   │   └── errors.go          # Error types
│   │
│   ├── storage/               # Storage layer
│   │   ├── backend.go         # Backend interface
│   │   ├── disk.go            # Disk backend
│   │   ├── memory.go          # Memory backend
│   │   ├── page.go            # Page types & layout
│   │   ├── pager.go           # Page read/write
│   │   ├── buffer_pool.go     # Page cache (LRU)
│   │   ├── wal.go             # Write-Ahead Log
│   │   ├── freelist.go        # Free page management
│   │   └── meta.go            # Meta page handling
│   │
│   ├── btree/                 # B+Tree implementation
│   │   ├── btree.go           # Core B+Tree
│   │   ├── node.go            # Node encoding/decoding
│   │   ├── iterator.go        # Range scan iterator
│   │   ├── split.go           # Node splitting
│   │   └── merge.go           # Node merging
│   │
│   ├── index/                 # Index management
│   │   ├── manager.go         # Index lifecycle
│   │   ├── btree_index.go     # B+Tree index
│   │   ├── hash_index.go      # Hash index
│   │   └── json_index.go      # JSON path index
│   │
│   ├── txn/                   # Transaction management
│   │   ├── manager.go         # MVCC transaction manager
│   │   ├── transaction.go     # Transaction struct
│   │   └── conflict.go        # Conflict detection
│   │
│   ├── query/                 # Query engine
│   │   ├── lexer.go           # SQL tokenizer
│   │   ├── token.go           # Token types
│   │   ├── parser.go          # SQL parser → AST
│   │   ├── ast.go             # AST node types
│   │   ├── planner.go         # Logical plan generation
│   │   ├── optimizer.go       # Plan optimization
│   │   ├── executor.go        # Iterator-based execution
│   │   └── expr.go            # Expression evaluation
│   │
│   ├── json/                  # JSON engine
│   │   ├── parser.go          # Fast JSON parser
│   │   ├── path.go            # JSON path evaluation
│   │   ├── msgpack.go         # MessagePack encoding
│   │   └── functions.go       # json_set, json_extract, etc.
│   │
│   ├── catalog/               # System catalog
│   │   ├── catalog.go         # Table/index definitions
│   │   └── schema.go          # Schema types
│   │
│   ├── server/                # Network server
│   │   ├── server.go          # TCP listener
│   │   ├── protocol.go        # Wire protocol
│   │   ├── handler.go         # Request handler
│   │   └── http.go            # REST API handler
│   │
│   └── wire/                  # Wire protocol codec
│       ├── codec.go           # MessagePack encode/decode
│       └── types.go           # Protocol message types
│
├── sdk/
│   ├── typescript/            # TypeScript/Node.js SDK
│   │   ├── src/
│   │   ├── package.json
│   │   └── tsconfig.json
│   │
│   └── python/               # Python SDK
│       ├── cobaltdb/
│       └── setup.py
│
├── internal/
│   └── testutil/              # Test helpers
│
├── test/
│   ├── integration/           # Integration tests
│   └── benchmark/             # Benchmark suite
│
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## 13. Implementation Roadmap

### Phase 1 — Storage Foundation (2-3 hafta)
- [ ] Backend interface + Disk + Memory implementations
- [ ] Page layout, header, meta page
- [ ] Pager (read/write pages)
- [ ] B+Tree: Get, Put, Delete, Iterator
- [ ] B+Tree: Split & merge
- [ ] Buffer pool with LRU eviction
- [ ] Free page list management
- [ ] Basic WAL (append, replay)
- [ ] Unit tests for all storage components

### Phase 2 — Query Engine (2-3 hafta)
- [ ] Lexer (tokenizer)
- [ ] Parser (SQL subset → AST)
- [ ] System catalog (CREATE TABLE, metadata)
- [ ] Basic executor: SeqScan, Filter, Projection
- [ ] INSERT, SELECT, UPDATE, DELETE execution
- [ ] Expression evaluator (comparisons, AND/OR, functions)
- [ ] Prepared statements & parameter binding

### Phase 3 — JSON & Transactions (2 hafta)
- [ ] JSON column type
- [ ] JSON path extraction (->>, ->)
- [ ] JSON functions (json_set, json_extract, etc.)
- [ ] MessagePack internal encoding
- [ ] MVCC transaction manager
- [ ] Snapshot isolation
- [ ] Conflict detection & retry
- [ ] WAL checkpoint & recovery

### Phase 4 — Indexes & Optimization (1-2 hafta)
- [ ] Secondary B+Tree indexes
- [ ] JSON path indexes
- [ ] Query planner (filter pushdown, index selection)
- [ ] Hash index (optional)
- [ ] EXPLAIN output

### Phase 5 — Server & SDKs (2 hafta)
- [ ] TCP server + MessagePack protocol
- [ ] REST/HTTP API
- [ ] Go client SDK (remote mode)
- [ ] TypeScript SDK
- [ ] Python SDK
- [ ] CLI REPL (interactive shell)

### Phase 6 — Polish & Optimization (ongoing)
- [ ] Comprehensive benchmark suite
- [ ] Fuzz testing (parser, B+Tree)
- [ ] Memory profiling & optimization
- [ ] Documentation
- [ ] database/sql driver compatibility

---

## 14. Diferansiyatörler — Neden CobaltDB?

| Feature             | SQLite    | BoltDB   | BadgerDB | CobaltDB        |
|---------------------|-----------|----------|----------|--------------|
| Language            | C         | Go       | Go       | **Go**       |
| Query Language      | SQL       | None     | None     | **SQL + JSON**|
| JSON Support        | Extension | None     | None     | **First-class**|
| Document Mode       | No        | No       | No       | **Yes**      |
| In-Memory Mode      | Yes       | No       | Yes      | **Yes**      |
| MVCC Transactions   | WAL mode  | MVCC     | MVCC     | **MVCC**     |
| Server Mode         | No        | No       | No       | **Yes**      |
| Remote SDKs         | No        | No       | No       | **Yes**      |
| Embeddable          | Yes       | Yes      | Yes      | **Yes**      |
| CGO Required        | Yes       | No       | No       | **No**       |
| JSON Path Indexes   | No        | No       | No       | **Yes**      |

---

## 15. Key Design Decisions

1. **B+Tree over LSM-Tree:** Read-heavy workload'lar için daha iyi. Write amplification
   LSM'den fazla ama read amplification çok daha az. SQLite de B+Tree kullanıyor.

2. **Page-based storage:** Predictable I/O, buffer pool ile cache kontrolü. Memory-mapped
   I/O yerine explicit read/write — Go runtime ile daha uyumlu.

3. **MessagePack for JSON storage:** Raw JSON text'ten ~40% daha compact, parse etmek
   daha hızlı. Dışarıya JSON olarak expose edilir ama internal'de binary.

4. **Rule-based optimizer:** İlk versiyon için cost-based optimizer gereksiz karmaşıklık.
   Filter pushdown + index selection yeterli. İleride statistics-based upgrade yapılabilir.

5. **Snapshot Isolation default:** Serializable çok pahalı, Read Committed çok weak.
   SI iyi bir denge — concurrent read'ler block olmaz, write conflict'ler detect edilir.

6. **Single-file format:** SQLite gibi tek `.cobalt` dosyası + `.cobalt.wal`. Backup = dosya kopyala.
   Deployment basitliği çok önemli.
