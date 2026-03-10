# CobaltDB Fix Guide - GLM

Bu dosya, CobaltDB kod tabanında tespit edilen tüm sorunların çözümlerini içerir.

---

## İçindekiler

1. [Critical Fixes](#1-critical-fixes)
2. [Security Fixes](#2-security-fixes)
3. [Concurrency Fixes](#3-concurrency-fixes)
4. [Type Safety Fixes](#4-type-safety-fixes)
5. [Error Handling Fixes](#5-error-handling-fixes)
6. [Performance Fixes](#6-performance-fixes)
7. [Resource Management Fixes](#7-resource-management-fixes)
8. [Code Quality Fixes](#8-code-quality-fixes)

---

## 1. Critical Fixes

### 1.1 Type Assertion Panic Fix

**Dosya:** `pkg/catalog/catalog_core.go`

**Mevcut Kod (Satır 2737):**
```go
case float64:
    f := v.(float64)
    return f == float64(int64(f)) && f >= -1e15 && f <= 1e15
```

**Düzeltilmiş Kod:**
```go
case f := float64:
    return f == float64(int64(f)) && f >= -1e15 && f <= 1e15
```

**Mevcut Kod (Satır 4044):**
```go
f := evalArgs[0].(float64)
```

**Düzeltilmiş Kod:**
```go
f, ok := evalArgs[0].(float64)
if !ok {
    return nil, fmt.Errorf("TYPEOF: expected float64 argument")
}
```

---

**Dosya:** `pkg/catalog/catalog_aggregate.go`

**Mevcut Kod (Satır 695-696):**
```go
viS := vi.(string)
vjS := vj.(string)
```

**Düzeltilmiş Kod:**
```go
viS, okI := vi.(string)
vjS, okJ := vj.(string)
if !okI || !okJ {
    return false
}
```

---

**Dosya:** `pkg/catalog/catalog_eval.go`

**Mevcut Kod (Satır 847):**
```go
f := evalArgs[0].(float64)
```

**Düzeltilmiş Kod:**
```go
f, ok := evalArgs[0].(float64)
if !ok {
    return nil, fmt.Errorf("expected float64 argument")
}
```

---

### 1.2 JSON Functions - Comma-Ok Pattern

**Dosya:** `pkg/catalog/catalog_core.go`

**Mevcut Kod (Satır 3161-3163):**
```go
case "JSON_SET":
    jsonData, _ := args[0].(string)
    path, _ := args[1].(string)
    value, _ := args[2].(string)
```

**Düzeltilmiş Kod:**
```go
case "JSON_SET":
    if len(args) < 3 {
        return nil, fmt.Errorf("JSON_SET requires 3 arguments")
    }
    jsonData, ok := args[0].(string)
    if !ok {
        return nil, fmt.Errorf("JSON_SET: first argument must be string")
    }
    path, ok := args[1].(string)
    if !ok {
        return nil, fmt.Errorf("JSON_SET: path must be string")
    }
    value, ok := args[2].(string)
    if !ok {
        return nil, fmt.Errorf("JSON_SET: value must be string")
    }
```

**Aynı pattern'i şu lokasyonlara uygula:**
- `catalog_core.go:3174-3175` (JSON_REMOVE)
- `catalog_core.go:3221` (JSON_EXTRACT)
- `catalog_core.go:3268-3269` (JSON_MERGE)
- `catalog_core.go:3302-3315` (REGEXP fonksiyonları)
- `catalog_core.go:3325-3326` (REGEXP_LIKE)

---

### 1.3 Slice Bounds Check - catalog_select.go

**Dosya:** `pkg/catalog/catalog_select.go`

**Mevcut Kod (Satır 446):**
```go
tableOffsets = append(tableOffsets, tableOffset{
    name:   joinAlias,
    offset: tableOffsets[len(tableOffsets)-1].offset + tableOffsets[len(tableOffsets)-1].count,
    count:  len(joinTableCols),
})
```

**Düzeltilmiş Kod:**
```go
if len(tableOffsets) == 0 {
    tableOffsets = append(tableOffsets, tableOffset{
        name:   joinAlias,
        offset: 0,
        count:  len(joinTableCols),
    })
} else {
    tableOffsets = append(tableOffsets, tableOffset{
        name:   joinAlias,
        offset: tableOffsets[len(tableOffsets)-1].offset + tableOffsets[len(tableOffsets)-1].count,
        count:  len(joinTableCols),
    })
}
```

---

## 2. Security Fixes

### 2.1 Default Admin Password - Require Change

**Dosya:** `cmd/cobaltdb-server/main.go`

**Mevcut Kod (Satır 26):**
```go
adminPass = flag.String("admin-pass", "admin", "default admin password")
```

**Düzeltilmiş Kod:**
```go
adminPass = flag.String("admin-pass", "", "admin password (required if auth enabled)")
```

**Dosya:** `pkg/server/server.go`

**Mevcut Kod (Satır 65):**
```go
DefaultAdminPass: "admin",
```

**Düzeltilmiş Kod:**
```go
DefaultAdminPass: "", // Must be set explicitly
```

**Ekle (Satır ~70):**
```go
// Validate admin password
if opts.AuthEnabled && opts.DefaultAdminPass == "" {
    return nil, fmt.Errorf("admin password must be set when auth is enabled (use --admin-pass flag)")
}

// Generate random password if not set and auth disabled
if !opts.AuthEnabled && opts.DefaultAdminPass == "" {
    randPass := generateRandomPassword(16)
    log.Printf("[WARN] No admin password set, generated: %s", randPass)
    opts.DefaultAdminPass = randPass
}

func generateRandomPassword(length int) string {
    const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%"
    b := make([]byte, length)
    rand.Read(b)
    for i := range b {
        b[i] = charset[int(b[i])%len(charset)]
    }
    return string(b)
}
```

---

### 2.2 Session Token Security

**Dosya:** `pkg/auth/auth.go`

**Mevcut Kod (Satır 243-253):**
```go
func generateToken(username string) (string, error) {
    b := make([]byte, 32)
    rand.Read(b)
    h := sha256.New()
    h.Write(b)
    h.Write([]byte(fmt.Sprintf("%s:%d", username, time.Now().UnixNano())))
    return hex.EncodeToString(h.Sum(nil)), nil
}
```

**Düzeltilmiş Kod:**
```go
func generateToken(username string) (string, error) {
    b := make([]byte, 32)
    if _, err := rand.Read(b); err != nil {
        return "", fmt.Errorf("failed to generate random bytes: %w", err)
    }
    // Use only random bytes for token - no predictable input
    return hex.EncodeToString(b), nil
}
```

---

### 2.3 SQL Injection Prevention - Stats Collector

**Dosya:** `pkg/catalog/stats.go`

**Mevcut Kod (Satır 100):**
```go
result, err := sc.catalog.ExecuteQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(tableName)))
```

**Düzeltilmiş Kod:**
```go
// Add identifier validation
func validateIdentifier(name string) error {
    if name == "" {
        return fmt.Errorf("empty identifier")
    }
    if len(name) > 64 {
        return fmt.Errorf("identifier too long")
    }
    // Only allow alphanumeric and underscore
    for _, r := range name {
        if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
            return fmt.Errorf("invalid character in identifier: %q", r)
        }
    }
    // Check for SQL keywords
    upperName := strings.ToUpper(name)
    sqlKeywords := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "UNION", "--", "/*"}
    for _, kw := range sqlKeywords {
        if strings.Contains(upperName, kw) {
            return fmt.Errorf("potential SQL injection detected")
        }
    }
    return nil
}

func (sc *StatsCollector) collectTableStats(tableName string) error {
    if err := validateIdentifier(tableName); err != nil {
        return fmt.Errorf("invalid table name: %w", err)
    }
    result, err := sc.catalog.ExecuteQuery(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(tableName)))
    // ...
}
```

---

### 2.4 Remove InsecureSkipVerify Option

**Dosya:** `pkg/server/tls.go`

**Mevcut Kod (Satır 34):**
```go
InsecureSkipVerify bool // WARNING: Only for development/testing. Never enable in production.
```

**Düzeltilmiş Kod:**
```go
// InsecureSkipVerify is removed - certificate verification is always enforced
// For development, use self-signed certificates with proper CA setup
```

**Sil (Satır ~150):**
```go
// Remove all references to InsecureSkipVerify
```

**Alternatif - Environment Variable ile Zorunlu Opt-in:**
```go
// Only allow InsecureSkipVerify via environment variable with explicit acknowledgment
if os.Getenv("COBALTDB_ALLOW_INSECURE_TLS") == "I_UNDERSTAND_THE_RISKS" {
    tlsConfig.InsecureSkipVerify = true
    log.Printf("[CRITICAL] TLS certificate verification DISABLED - NOT SAFE FOR PRODUCTION")
}
```

---

### 2.5 io.ReadAll with Size Limit

**Dosya:** `pkg/server/admin_test.go`

**Mevcut Kod (Satır 43):**
```go
body, _ := io.ReadAll(resp.Body)
```

**Düzeltilmiş Kod:**
```go
const maxResponseBodySize = 10 * 1024 * 1024 // 10MB

body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
if err != nil {
    t.Fatalf("Failed to read response: %v", err)
}
```

---

## 3. Concurrency Fixes

### 3.1 MySQL Server WaitGroup

**Dosya:** `pkg/protocol/mysql.go`

**Mevcut Kod:**
```go
type MySQLServer struct {
    addr   string
    server *net.TCPListener
    db     *engine.DB
    mu     sync.Mutex
    done   chan struct{}
}
```

**Düzeltilmiş Kod:**
```go
type MySQLServer struct {
    addr      string
    server    *net.TCPListener
    db        *engine.DB
    mu        sync.Mutex
    done      chan struct{}
    clientWg  sync.WaitGroup  // ADD THIS
    ctx       context.Context  // ADD THIS
    cancel    context.CancelFunc // ADD THIS
}
```

**NewMySQLServer fonksiyonuna ekle:**
```go
func NewMySQLServer(addr string, db *engine.DB) *MySQLServer {
    ctx, cancel := context.WithCancel(context.Background())
    return &MySQLServer{
        addr:   addr,
        db:     db,
        done:   make(chan struct{}),
        ctx:    ctx,
        cancel: cancel,
    }
}
```

**HandleConnection (Satır 164) değiştir:**
```go
// OLD
go s.handleConnection(conn)

// NEW
s.clientWg.Add(1)
go func() {
    defer s.clientWg.Done()
    s.handleConnection(conn)
}()
```

**Close fonksiyonunu güncelle:**
```go
func (s *MySQLServer) Close() error {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    close(s.done)
    s.cancel() // Cancel all contexts
    
    if s.server != nil {
        s.server.Close()
    }
    
    // Wait for all client handlers to finish
    done := make(chan struct{})
    go func() {
        s.clientWg.Wait()
        close(done)
    }()
    
    select {
    case <-done:
        return nil
    case <-time.After(30 * time.Second):
        return fmt.Errorf("timeout waiting for client handlers to finish")
    }
}
```

---

### 3.2 MySQL Query Cancellation

**Dosya:** `pkg/protocol/mysql.go`

**Mevcut Kod (Satır 467):**
```go
ctx := context.Background()
rows, err := c.server.db.Query(ctx, sql)
```

**Düzeltilmiş Kod:**
```go
// Add context to client connection
type clientConn struct {
    conn      net.Conn
    server    *MySQLServer
    ctx       context.Context
    cancel    context.CancelFunc
}

func (c *clientConn) handle() {
    defer c.cancel()
    // ... existing code ...
    
    // Use cancellable context
    rows, err := c.server.db.Query(c.ctx, sql)
    // ...
}

// Cancel on client disconnect
func (c *clientConn) readPacket() ([]byte, error) {
    // Set read deadline
    c.conn.SetReadDeadline(time.Now().Add(30 * time.Second))
    // ... existing code ...
}
```

---

### 3.3 Auth Duplicate Session Cleanup

**Dosya:** `pkg/auth/auth.go`

**Mevcut Kod (Satır 71):**
```go
func NewAuthenticator() *Authenticator {
    a := &Authenticator{
        // ...
    }
    go a.sessionCleanupLoop()
    return a
}
```

**Düzeltilmiş Kod:**
```go
type Authenticator struct {
    // ... existing fields ...
    cleanupStarted bool
    cleanupMu      sync.Mutex
}

func NewAuthenticator() *Authenticator {
    a := &Authenticator{
        // ...
        cleanupStarted: false,
    }
    // Don't start cleanup here - let caller decide
    return a
}

func (a *Authenticator) StartSessionCleanup() {
    a.cleanupMu.Lock()
    defer a.cleanupMu.Unlock()
    
    if a.cleanupStarted {
        return // Already started
    }
    a.cleanupStarted = true
    
    go func() {
        defer func() {
            if r := recover(); r != nil {
                log.Printf("[Auth] Panic in session cleanup: %v", r)
            }
        }()
        a.sessionCleanupLoop()
    }()
}
```

---

### 3.4 Admin Server WaitGroup

**Dosya:** `pkg/server/admin.go`

**Mevcut Kod:**
```go
type AdminServer struct {
    // ...
}
```

**Düzeltilmiş Kod:**
```go
type AdminServer struct {
    server *http.Server
    addr   string
    wg     sync.WaitGroup  // ADD
    done   chan struct{}   // ADD
}

func (a *AdminServer) Start() error {
    // ... existing code ...
    
    a.wg.Add(1)
    go func() {
        defer func() {
            a.wg.Done()
            if r := recover(); r != nil {
                log.Printf("[Admin] Panic: %v", r)
            }
        }()
        log.Printf("[Admin] Starting admin server on %s", a.addr)
        if err := a.server.Serve(listener); err != nil && err != http.ErrServerClosed {
            log.Printf("[Admin] Server error: %v", err)
        }
    }()
    
    return nil
}

func (a *AdminServer) Stop() error {
    close(a.done)
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    
    err := a.server.Shutdown(ctx)
    a.wg.Wait()  // Wait for goroutine to finish
    return err
}
```

---

### 3.5 Audit Logger Double-Close Prevention

**Dosya:** `pkg/audit/logger.go`

**Mevcut Kod:**
```go
func (al *Logger) Close() error {
    if !al.config.Enabled {
        return nil
    }
    close(al.stopChan)
    al.wg.Wait()
    // ...
}
```

**Düzeltilmiş Kod:**
```go
type Logger struct {
    // ... existing fields ...
    closeOnce sync.Once
    closed    bool
    closeMu   sync.RWMutex
}

func (al *Logger) Close() error {
    if !al.config.Enabled {
        return nil
    }
    
    var err error
    al.closeOnce.Do(func() {
        al.closeMu.Lock()
        al.closed = true
        al.closeMu.Unlock()
        
        close(al.stopChan)
        al.wg.Wait()
        
        if al.file != nil {
            err = al.file.Close()
        }
    })
    return err
}

func (al *Logger) log(event AuditEvent) {
    al.closeMu.RLock()
    closed := al.closed
    al.closeMu.RUnlock()
    
    if closed {
        return
    }
    // ... rest of logging
}
```

---

### 3.6 Circuit Breaker Goroutine Leak Fix

**Dosya:** `pkg/engine/circuit_breaker.go`

**Mevcut Kod (Satır 260-282):**
```go
done := make(chan error, 1)
go func() {
    done <- fn()
}()

select {
case err := <-done:
    // ...
case <-ctx.Done():
    cb.ReportFailure()
    go func() { <-done }()  // Drain goroutine
    return ctx.Err()
}
```

**Düzeltilmiş Kod:**
```go
// Add timeout for stuck operations
done := make(chan error, 1)
go func() {
    defer func() {
        if r := recover(); r != nil {
            done <- fmt.Errorf("panic in circuit breaker: %v", r)
        }
    }()
    done <- fn()
}()

select {
case err := <-done:
    return err
case <-ctx.Done():
    cb.ReportFailure()
    // Wait for fn to complete with timeout
    select {
    case <-done:
        // fn completed after context cancellation
    case <-time.After(5 * time.Second):
        log.Printf("[CircuitBreaker] WARNING: fn() did not return after context cancellation")
    }
    return ctx.Err()
}
```

---

## 4. Type Safety Fixes

### 4.1 Integer Overflow Prevention

**Dosya:** `pkg/catalog/catalog_core.go`

**Mevcut Kod (Satır 1863):**
```go
return int(val), true
```

**Düzeltilmiş Kod:**
```go
const (
    maxInt = int64(^uint(0) >> 1)
    minInt = -maxInt - 1
)

func toInt(val interface{}) (int, bool) {
    switch v := val.(type) {
    case int:
        return v, true
    case int64:
        if v > maxInt || v < minInt {
            return 0, false // Overflow
        }
        return int(v), true
    case float64:
        if v > float64(maxInt) || v < float64(minInt) {
            return 0, false // Overflow
        }
        return int(v), true
    // ...
    }
    return 0, false
}
```

---

### 4.2 Float Precision in JSON Index

**Dosya:** `pkg/catalog/catalog_json.go`

**Mevcut Kod (Satır 129):**
```go
idx.NumIndex[float64(v)]
```

**Düzeltilmiş Kod:**
```go
// Use string key for integers to avoid float precision issues
func float64Key(f float64) string {
    // For integers that fit in int64, use integer representation
    if f == float64(int64(f)) && f >= float64(math.MinInt64) && f <= float64(math.MaxInt64) {
        return strconv.FormatInt(int64(f), 10)
    }
    // For true floats, use full precision
    return strconv.FormatFloat(f, 'g', -1, 64)
}

// Usage:
idx.NumIndex[float64Key(v)] = append(idx.NumIndex[float64Key(v)], rowNum)
```

---

### 4.3 Map Access with Comma-Ok

**Dosya:** `pkg/catalog/catalog_json.go`

**Mevcut Kod (Satır 102):**
```go
current = curr[key]
```

**Düzeltilmiş Kod:**
```go
next, exists := curr[key]
if !exists {
    return nil, fmt.Errorf("JSON path key not found: %s", key)
}
current = next
```

---

## 5. Error Handling Fixes

### 5.1 Error Wrapping %w Verb

**Dosya:** `pkg/storage/encryption.go`

**Mevcut Kod (Satır 68):**
```go
return fmt.Errorf("%w: %v", ErrEncryptionFailed, err)
```

**Düzeltilmiş Kod:**
```go
return fmt.Errorf("%w: %w", ErrEncryptionFailed, err)
```

**Tüm dosya için değiştirilecek satırlar:**
- Line 68: `%v` → `%w`
- Line 73: `%v` → `%w`
- Line 89: `%v` → `%w`
- Line 153: `%v` → `%w`
- Line 173: `%v` → `%w`

---

**Dosya:** `pkg/server/tls.go`

**Değiştirilecek satırlar:**
- Line 85: `%v` → `%w`
- Line 128: `%v` → `%w`

---

**Dosya:** `pkg/security/rls.go`

**Değiştir (Satır 155):**
```go
return fmt.Errorf("%w: %w", ErrInvalidPolicy, err)
```

---

**Dosya:** `pkg/catalog/catalog_update.go`

**Değiştir (Satır 159):**
```go
return fmt.Errorf("CHECK constraint failed: %w", err)
```

---

**Dosya:** `pkg/catalog/catalog_insert.go`

**Değiştir (Satır 295):**
```go
return fmt.Errorf("CHECK constraint failed: %w", err)
```

---

### 5.2 Panic Recovery in Auth Cleanup

**Dosya:** `pkg/auth/auth.go`

**Mevcut Kod (Satır 467-478):**
```go
func (a *Authenticator) StartSessionCleanup() {
    go func() {
        ticker := time.NewTicker(5 * time.Minute)
        for range ticker.C {
            a.CleanupExpiredSessions()
        }
    }()
}
```

**Düzeltilmiş Kod:**
```go
func (a *Authenticator) StartSessionCleanup() {
    a.cleanupMu.Lock()
    if a.cleanupStarted {
        a.cleanupMu.Unlock()
        return
    }
    a.cleanupStarted = true
    a.cleanupMu.Unlock()
    
    go func() {
        defer func() {
            if r := recover(); r != nil {
                log.Printf("[Auth] Panic in session cleanup: %v", r)
                // Restart cleanup after panic
                time.Sleep(1 * time.Minute)
                a.cleanupStarted = false
                a.StartSessionCleanup()
            }
        }()
        
        ticker := time.NewTicker(5 * time.Minute)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                a.CleanupExpiredSessions()
            case <-a.stopCleanup:
                return
            }
        }
    }()
}
```

---

### 5.3 Missing Error Context

**Dosya:** `pkg/server/tls.go`

**Mevcut Kod (Satır 147):**
```go
return err
```

**Düzeltilmiş Kod:**
```go
return fmt.Errorf("failed to create certificate directory %q: %w", certDir, err)
```

---

### 5.4 Ignored Close Errors

**Dosya:** `pkg/server/tls.go`

**Mevcut Kod (Satır 209, 220):**
```go
defer certOut.Close()
defer keyOut.Close()
```

**Düzeltilmiş Kod:**
```go
defer func() {
    if err := certOut.Close(); err != nil {
        log.Printf("[TLS] Failed to close cert file: %v", err)
    }
}()
defer func() {
    if err := keyOut.Close(); err != nil {
        log.Printf("[TLS] Failed to close key file: %v", err)
    }
}()
```

---

## 6. Performance Fixes

### 6.1 UNIQUE Constraint - Use Index

**Dosya:** `pkg/catalog/catalog_insert.go`

**Mevcut Kod (Satır 229-248):**
```go
for i, col := range table.Columns {
    if col.Unique && rowValues[i] != nil {
        iter, _ := tree.Scan(nil, nil)
        for iter.HasNext() {
            // Full table scan - O(n)
        }
    }
}
```

**Düzeltilmiş Kod:**
```go
// Add unique index structure to Table
type Table struct {
    // ... existing fields ...
    UniqueIndexes map[int]*UniqueIndex // column index -> unique index
}

type UniqueIndex struct {
    Values map[interface{}]string // value -> primary key
    mu     sync.RWMutex
}

// During INSERT
func (c *Catalog) validateUniqueConstraints(table *Table, rowValues []interface{}, pk string) error {
    for colIdx, col := range table.Columns {
        if !col.Unique || rowValues[colIdx] == nil {
            continue
        }
        
        value := rowValues[colIdx]
        
        // O(1) lookup instead of O(n) scan
        if idx, exists := table.UniqueIndexes[colIdx]; exists {
            idx.mu.RLock()
            existingPK, found := idx.Values[value]
            idx.mu.RUnlock()
            
            if found && existingPK != pk {
                return fmt.Errorf("UNIQUE constraint violation: column %s, value %v already exists", 
                    col.Name, value)
            }
        }
    }
    return nil
}

// Update index after successful insert
func (c *Catalog) updateUniqueIndexes(table *Table, rowValues []interface{}, pk string) {
    for colIdx, col := range table.Columns {
        if !col.Unique || rowValues[colIdx] == nil {
            continue
        }
        if idx, exists := table.UniqueIndexes[colIdx]; exists {
            idx.mu.Lock()
            idx.Values[rowValues[colIdx]] = pk
            idx.mu.Unlock()
        }
    }
}
```

---

### 6.2 Binary Row Encoding (Replace JSON)

**Dosya:** `pkg/catalog/catalog_core.go`

**Yeni Fonksiyonlar:**
```go
// Binary encoding format:
// [2 bytes: num columns][for each column: 1 byte type + data]

const (
    typeNull    = 0
    typeInt64   = 1
    typeFloat64 = 2
    typeString  = 3
    typeBool    = 4
    typeBytes   = 5
)

// Encode row to binary - no reflection
func fastEncodeRowV2(row []interface{}) ([]byte, error) {
    buf := make([]byte, 2, 64) // Start with 2 bytes for count
    binary.BigEndian.PutUint16(buf, uint16(len(row)))
    
    for _, val := range row {
        switch v := val.(type) {
        case nil:
            buf = append(buf, typeNull)
        case int64:
            buf = append(buf, typeInt64)
            buf = append(buf, make([]byte, 8)...)
            binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(v))
        case int:
            buf = append(buf, typeInt64)
            buf = append(buf, make([]byte, 8)...)
            binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(v))
        case float64:
            buf = append(buf, typeFloat64)
            buf = append(buf, make([]byte, 8)...)
            binary.BigEndian.PutUint64(buf[len(buf)-8:], math.Float64bits(v))
        case string:
            buf = append(buf, typeString)
            strBytes := []byte(v)
            if len(strBytes) > 65535 {
                strBytes = strBytes[:65535]
            }
            buf = append(buf, byte(len(strBytes)>>8), byte(len(strBytes)))
            buf = append(buf, strBytes...)
        case bool:
            if v {
                buf = append(buf, typeBool, 1)
            } else {
                buf = append(buf, typeBool, 0)
            }
        case []byte:
            buf = append(buf, typeBytes)
            if len(v) > 65535 {
                v = v[:65535]
            }
            buf = append(buf, byte(len(v)>>8), byte(len(v)))
            buf = append(buf, v...)
        default:
            return nil, fmt.Errorf("unsupported type: %T", val)
        }
    }
    return buf, nil
}

// Decode binary to row - no reflection
func fastDecodeRowV2(data []byte) ([]interface{}, error) {
    if len(data) < 2 {
        return nil, fmt.Errorf("data too short")
    }
    
    numCols := binary.BigEndian.Uint16(data[:2])
    data = data[2:]
    row := make([]interface{}, 0, numCols)
    
    for i := 0; i < int(numCols) && len(data) > 0; i++ {
        typ := data[0]
        data = data[1:]
        
        switch typ {
        case typeNull:
            row = append(row, nil)
        case typeInt64:
            if len(data) < 8 {
                return nil, fmt.Errorf("unexpected end of data for int64")
            }
            row = append(row, int64(binary.BigEndian.Uint64(data[:8])))
            data = data[8:]
        case typeFloat64:
            if len(data) < 8 {
                return nil, fmt.Errorf("unexpected end of data for float64")
            }
            row = append(row, math.Float64frombits(binary.BigEndian.Uint64(data[:8])))
            data = data[8:]
        case typeString:
            if len(data) < 2 {
                return nil, fmt.Errorf("unexpected end of data for string length")
            }
            strLen := int(data[0])<<8 | int(data[1])
            data = data[2:]
            if len(data) < strLen {
                return nil, fmt.Errorf("unexpected end of data for string")
            }
            row = append(row, string(data[:strLen]))
            data = data[strLen:]
        case typeBool:
            if len(data) < 1 {
                return nil, fmt.Errorf("unexpected end of data for bool")
            }
            row = append(row, data[0] != 0)
            data = data[1:]
        case typeBytes:
            if len(data) < 2 {
                return nil, fmt.Errorf("unexpected end of data for bytes length")
            }
            bytesLen := int(data[0])<<8 | int(data[1])
            data = data[2:]
            if len(data) < bytesLen {
                return nil, fmt.Errorf("unexpected end of data for bytes")
            }
            row = append(row, data[:bytesLen])
            data = data[bytesLen:]
        default:
            return nil, fmt.Errorf("unknown type byte: %d", typ)
        }
    }
    
    return row, nil
}
```

---

### 6.3 Regex Compilation Cache

**Dosya:** `pkg/security/rls.go`

**Mevcut Kod (Satır 863):**
```go
func parseInOperator(expr string) PolicyExpr {
    inRegex := regexp.MustCompile(`(?i)^(.+?)\s+IN\s*\((.+?)\)$`)
    // ...
}
```

**Düzeltilmiş Kod:**
```go
// Package-level regex cache
var (
    regexCache   = make(map[string]*regexp.Regexp)
    regexCacheMu sync.RWMutex
)

func getCompiledRegex(pattern string) (*regexp.Regexp, error) {
    regexCacheMu.RLock()
    re, exists := regexCache[pattern]
    regexCacheMu.RUnlock()
    
    if exists {
        return re, nil
    }
    
    regexCacheMu.Lock()
    defer regexCacheMu.Unlock()
    
    // Double check
    if re, exists = regexCache[pattern]; exists {
        return re, nil
    }
    
    re, err := regexp.Compile(pattern)
    if err != nil {
        return nil, err
    }
    
    regexCache[pattern] = re
    return re, nil
}

// Pre-compile common patterns at init
func init() {
    patterns := []string{
        `(?i)^(.+?)\s+IN\s*\((.+?)\)$`,
        `(?i)^(.+?)\s+LIKE\s+['"](.+?)['"]$`,
        // Add other common patterns
    }
    for _, p := range patterns {
        getCompiledRegex(p)
    }
}
```

---

### 6.4 Hash Join Implementation

**Dosya:** `pkg/catalog/catalog_select.go`

**Yeni Fonksiyon:**
```go
// Hash join for large tables
func hashJoin(leftRows, rightRows [][]interface{}, leftCol, rightCol int) [][]interface{} {
    if len(leftRows) == 0 || len(rightRows) == 0 {
        return nil
    }
    
    // Build hash table from smaller table
    if len(leftRows) > len(rightRows) {
        // Swap to build from smaller table
        leftRows, rightRows = rightRows, leftRows
        leftCol, rightCol = rightCol, leftCol
    }
    
    // Build phase
    hashTable := make(map[interface{}][][]interface{})
    for _, row := range leftRows {
        key := row[leftCol]
        hashTable[key] = append(hashTable[key], row)
    }
    
    // Probe phase
    var result [][]interface{}
    for _, rightRow := range rightRows {
        key := rightRow[rightCol]
        if leftMatches, exists := hashTable[key]; exists {
            for _, leftRow := range leftMatches {
                combined := make([]interface{}, len(leftRow)+len(rightRow))
                copy(combined, leftRow)
                copy(combined[len(leftRow):], rightRow)
                result = append(result, combined)
            }
        }
    }
    
    return result
}
```

---

### 6.5 sync.Pool for Row Buffers

**Dosya:** `pkg/catalog/catalog_core.go`

**Yeni Kod:**
```go
var rowBufferPool = sync.Pool{
    New: func() interface{} {
        // Pre-allocate common row size
        return make([]interface{}, 0, 16)
    },
}

func getRowBuffer() []interface{} {
    return rowBufferPool.Get().([]interface{})[:0]
}

func putRowBuffer(buf []interface{}) {
    // Clear references to allow GC
    for i := range buf {
        buf[i] = nil
    }
    rowBufferPool.Put(buf[:0])
}

// Usage in hot paths:
func (c *Catalog) processRows(rows [][]interface{}) {
    buf := getRowBuffer()
    defer putRowBuffer(buf)
    
    for _, row := range rows {
        buf = append(buf, row...)
        // process...
        buf = buf[:0] // Reset for next row
    }
}
```

---

### 6.6 String Key Generation Without fmt.Sprintf

**Dosya:** `pkg/catalog/catalog_insert.go`

**Mevcut Kod (Satır 140):**
```go
key = fmt.Sprintf("%020d", pkVal)
```

**Düzeltilmiş Kod:**
```go
// Fast zero-padded integer to string
func formatKey(pkVal int64) string {
    const digits = "0123456789"
    var buf [20]byte
    
    for i := 19; i >= 0; i-- {
        buf[i] = digits[pkVal%10]
        pkVal /= 10
    }
    
    return string(buf[:])
}
```

---

### 6.7 String Builder in Logger

**Dosya:** `pkg/logger/logger.go`

**Mevcut Kod (Satır 213-231):**
```go
logLine := fmt.Sprintf("[%s] %s", timestamp, level.String())
if l.component != "" {
    logLine += fmt.Sprintf(" [%s]", l.component)
}
logLine += fmt.Sprintf(" %s", msg)
// ...
```

**Düzeltilmiş Kod:**
```go
func (l *Logger) formatLine(timestamp string, level LogLevel, msg string, err error) string {
    var sb strings.Builder
    sb.Grow(128) // Pre-allocate reasonable size
    
    sb.WriteByte('[')
    sb.WriteString(timestamp)
    sb.WriteString("] ")
    sb.WriteString(level.String())
    
    if l.component != "" {
        sb.WriteString(" [")
        sb.WriteString(l.component)
        sb.WriteByte(']')
    }
    
    sb.WriteByte(' ')
    sb.WriteString(msg)
    
    if err != nil {
        sb.WriteString(" | error=")
        sb.WriteString(err.Error())
    }
    
    for k, v := range l.fields {
        sb.WriteString(" | ")
        sb.WriteString(k)
        sb.WriteByte('=')
        fmt.Fprintf(&sb, "%v", v)
    }
    
    sb.WriteByte('\n')
    return sb.String()
}
```

---

## 7. Resource Management Fixes

### 7.1 String Concatenation in Loops

**Dosya:** `test/v94_insert_replace_bugs_test.go`

**Mevcut Kod (Satır 257-260):**
```go
longStr := ""
for i := 0; i < 1000; i++ {
    longStr += "abcdefghij"
}
```

**Düzeltilmiş Kod:**
```go
var sb strings.Builder
sb.Grow(10000) // Pre-allocate capacity
for i := 0; i < 1000; i++ {
    sb.WriteString("abcdefghij")
}
longStr := sb.String()

// Or simply:
longStr := strings.Repeat("abcdefghij", 1000)
```

---

### 7.2 Missing Transaction Rollback

**Dosya:** `cmd/demo-ecommerce/main.go`

**Mevcut Kod (Satır 94):**
```go
tx, _ := db.Begin(ctx)
for _, cat := range []string{...} {
    tx.Exec(ctx, "INSERT INTO categories (name) VALUES (?)", cat)
}
tx.Commit()
```

**Düzeltilmiş Kod:**
```go
tx, err := db.Begin(ctx)
if err != nil {
    log.Fatalf("Failed to begin transaction: %v", err)
}

// Always rollback if not committed
defer func() {
    if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
        log.Printf("Warning: rollback failed: %v", err)
    }
}()

for _, cat := range []string{...} {
    if _, err := tx.Exec(ctx, "INSERT INTO categories (name) VALUES (?)", cat); err != nil {
        log.Fatalf("Failed to insert category: %v", err) // defer will rollback
    }
}

if err := tx.Commit(); err != nil {
    log.Fatalf("Failed to commit: %v", err)
}
```

---

### 7.3 Unclosed Rows Fix

**Dosya:** `cmd/demo-ecommerce/main.go`

**Mevcut Kod (Satır 194-197):**
```go
rows, _ := db.Query(ctx, "SELECT status, COUNT(*), SUM(total) FROM orders GROUP BY status")
if rows != nil {
    rows.Close()
}
```

**Düzeltilmiş Kod:**
```go
rows, err := db.Query(ctx, "SELECT status, COUNT(*), SUM(total) FROM orders GROUP BY status")
if err != nil {
    log.Printf("Query failed: %v", err)
    return
}
defer rows.Close()

for rows.Next() {
    var status string
    var count int
    var total float64
    if err := rows.Scan(&status, &count, &total); err != nil {
        log.Printf("Scan failed: %v", err)
        return
    }
    // Process row...
}
```

---

## 8. Code Quality Fixes

### 8.1 Split catalog_core.go

**Yeni dosya yapısı:**

```
pkg/catalog/
├── catalog.go           # Core Catalog struct, New()
├── types.go             # Types: Table, Column, Index, etc.
├── query.go             # ExecuteQuery, selectLocked
├── eval.go              # evaluateExpression, eval helpers
├── functions.go         # Built-in functions
├── json_funcs.go        # JSON functions
├── encoding.go          # fastEncodeRow, decodeRow
├── cache.go             # QueryCache implementation
├── constraints.go       # Constraint validation
├── scan.go              # Table scanning logic
├── catalog_ddl.go       # DDL operations (existing)
├── catalog_select.go    # SELECT operations (existing)
├── catalog_insert.go    # INSERT operations (existing)
├── catalog_update.go    # UPDATE operations (existing)
├── catalog_delete.go    # DELETE operations (existing)
└── ...
```

**Taşınacak fonksiyonlar:**

| Source File | Target File | Functions |
|-------------|-------------|-----------|
| catalog_core.go | types.go | `Column`, `Table`, `Index`, `ForeignKey` types |
| catalog_core.go | eval.go | `evaluateExpression`, `evalBinaryExpr`, etc. |
| catalog_core.go | functions.go | All `case "UPPER"`, `case "LOWER"`, etc. |
| catalog_core.go | encoding.go | `fastEncodeRow`, `decodeRow` |
| catalog_core.go | cache.go | `QueryCache`, `Get`, `Set` |

---

### 8.2 EvalContext Struct

**Dosya:** `pkg/catalog/eval.go`

**Yeni Kod:**
```go
// EvalContext bundles common parameters for expression evaluation
type EvalContext struct {
    Catalog    *Catalog
    Row        []interface{}
    Columns    []Column
    Args       []interface{}
    TableName  string
    SelectCols []selectColInfo
}

// NewEvalContext creates an evaluation context
func NewEvalContext(c *Catalog, table string) *EvalContext {
    return &EvalContext{
        Catalog:   c,
        TableName: table,
    }
}

// WithRow sets the current row
func (ctx *EvalContext) WithRow(row []interface{}, cols []Column) *EvalContext {
    ctx.Row = row
    ctx.Columns = cols
    return ctx
}

// WithArgs sets query arguments
func (ctx *EvalContext) WithArgs(args []interface{}) *EvalContext {
    ctx.Args = args
    return ctx
}
```

**Kullanım:**
```go
// Old
func (c *Catalog) evaluateWhere(expr query.Expression, row []interface{}, columns []Column, 
    table string, args []interface{}, selectCols []selectColInfo) (bool, error)

// New
func (ctx *EvalContext) evaluateWhere(expr query.Expression) (bool, error)
```

---

### 8.3 Shared Test Utilities

**Dosya:** `test/helpers.go`

**Yeni Kod:**
```go
package test

import (
    "testing"
    "github.com/cobaltdb/cobaltdb/pkg/engine"
)

// TestDB creates a test database
func TestDB(t *testing.T) *engine.DB {
    t.Helper()
    db, err := engine.Open(":memory:")
    if err != nil {
        t.Fatalf("Failed to open test database: %v", err)
    }
    t.Cleanup(func() {
        db.Close()
    })
    return db
}

// ExpectRows asserts query returns expected rows
func ExpectRows(t *testing.T, db *engine.DB, query string, expected [][]interface{}) {
    t.Helper()
    rows, err := db.Query(context.Background(), query)
    if err != nil {
        t.Fatalf("Query failed: %v", err)
    }
    defer rows.Close()
    
    var actual [][]interface{}
    for rows.Next() {
        row, err := rows.Scan()
        if err != nil {
            t.Fatalf("Scan failed: %v", err)
        }
        actual = append(actual, row)
    }
    
    if len(actual) != len(expected) {
        t.Fatalf("Expected %d rows, got %d", len(expected), len(actual))
    }
    
    for i := range expected {
        if !rowsEqual(actual[i], expected[i]) {
            t.Fatalf("Row %d mismatch: expected %v, got %v", i, expected[i], actual[i])
        }
    }
}

// ExpectVal asserts query returns single value
func ExpectVal(t *testing.T, db *engine.DB, query string, expected interface{}) {
    t.Helper()
    var actual interface{}
    err := db.QueryRow(context.Background(), query).Scan(&actual)
    if err != nil {
        t.Fatalf("QueryRow failed: %v", err)
    }
    if actual != expected {
        t.Fatalf("Expected %v, got %v", expected, actual)
    }
}

// ExpectError asserts query fails with expected error
func ExpectError(t *testing.T, db *engine.DB, query string, expectedErr string) {
    t.Helper()
    _, err := db.Exec(context.Background(), query)
    if err == nil {
        t.Fatalf("Expected error containing %q, got nil", expectedErr)
    }
    if !strings.Contains(err.Error(), expectedErr) {
        t.Fatalf("Expected error containing %q, got %q", expectedErr, err.Error())
    }
}

func rowsEqual(a, b []interface{}) bool {
    if len(a) != len(b) {
        return false
    }
    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}
```

---

### 8.4 Add Documentation

**Dosya:** `pkg/catalog/catalog.go`

**Yeni Kod:**
```go
// Package catalog provides table management and query execution for CobaltDB.
// It handles table creation, indexing, constraints, and SQL query processing.
//
// The Catalog is the central component that manages all database metadata
// and provides the query execution engine.
//
// Example usage:
//
//     tree := btree.NewBTree()
//     catalog := catalog.New(tree, nil, nil)
//     
//     // Create a table
//     err := catalog.CreateTable("users", []catalog.Column{
//         {Name: "id", Type: "INTEGER", PrimaryKey: true},
//         {Name: "name", Type: "TEXT", NotNull: true},
//     })
//     
//     // Execute a query
//     result, err := catalog.ExecuteQuery("SELECT * FROM users WHERE id = ?", 1)
package catalog

// New creates a new Catalog instance with the given BTree storage backend.
// The buffer pool and WAL are optional; pass nil for in-memory operation.
//
// Parameters:
//   - tree: The B+tree storage backend for persisting data
//   - pool: Buffer pool for page caching (nil for default)
//   - wal: Write-ahead log for durability (nil for no logging)
//
// Returns a fully initialized Catalog ready for query execution.
func New(tree *btree.BTree, pool *storage.BufferPool, wal *storage.WAL) *Catalog {
    // ...
}
```

---

### 8.5 Functional Options Pattern

**Dosya:** `pkg/auth/auth.go`

**Mevcut Kod:**
```go
func (a *Authenticator) CreateUser(username, password string, isAdmin bool) error
```

**Düzeltilmiş Kod:**
```go
// UserOption configures user creation
type UserOption func(*userConfig)

type userConfig struct {
    isAdmin    bool
    expiresAt  time.Time
    metadata   map[string]string
}

// WithAdmin grants admin privileges
func WithAdmin() UserOption {
    return func(c *userConfig) {
        c.isAdmin = true
    }
}

// WithExpiration sets account expiration
func WithExpiration(t time.Time) UserOption {
    return func(c *userConfig) {
        c.expiresAt = t
    }
}

// WithMetadata sets user metadata
func WithMetadata(key, value string) UserOption {
    return func(c *userConfig) {
        if c.metadata == nil {
            c.metadata = make(map[string]string)
        }
        c.metadata[key] = value
    }
}

// CreateUser creates a new user with optional configuration
func (a *Authenticator) CreateUser(username, password string, opts ...UserOption) error {
    a.mu.Lock()
    defer a.mu.Unlock()
    
    // Apply defaults
    config := &userConfig{
        isAdmin:   false,
        metadata:  make(map[string]string),
    }
    
    // Apply options
    for _, opt := range opts {
        opt(config)
    }
    
    // Validate
    if username == "" {
        return errors.New("username cannot be empty")
    }
    if len(password) < 8 {
        return errors.New("password must be at least 8 characters")
    }
    
    // Check if user exists
    if _, exists := a.users[username]; exists {
        return ErrUserExists
    }
    
    // Hash password
    hash, err := a.hashPassword(password)
    if err != nil {
        return err
    }
    
    // Create user
    a.users[username] = &User{
        Username:  username,
        Password:  hash,
        IsAdmin:   config.isAdmin,
        ExpiresAt: config.expiresAt,
        Metadata:  config.metadata,
    }
    
    return nil
}

// Usage:
// auth.CreateUser("john", "password123")
// auth.CreateUser("admin", "securepass", WithAdmin())
// auth.CreateUser("temp", "temp123", WithExpiration(time.Now().Add(24*time.Hour)))
```

---

## 9. CI/CD Configuration

### 9.1 Pre-commit Hook

**Dosya:** `.git/hooks/pre-commit`

```bash
#!/bin/bash

echo "Running pre-commit checks..."

# Format check
echo "Checking gofmt..."
FILES=$(gofmt -l .)
if [ -n "$FILES" ]; then
    echo "Files need formatting:"
    echo "$FILES"
    exit 1
fi

# Lint
echo "Running go vet..."
go vet ./...
if [ $? -ne 0 ]; then
    exit 1
fi

# Tests
echo "Running tests..."
go test -short ./...
if [ $? -ne 0 ]; then
    exit 1
fi

echo "Pre-commit checks passed!"
```

---

### 9.2 Makefile Updates

**Dosya:** `Makefile`

```makefile
.PHONY: all build test lint fmt check security coverage clean

all: build

build:
	go build ./...

test:
	go test -v -race -coverprofile=coverage.out ./...

test-short:
	go test -short ./...

lint:
	go vet ./...
	@which golangci-lint > /dev/null && golangci-lint run ./...

fmt:
	gofmt -w .

check: fmt lint test

security:
	@which govulncheck > /dev/null && govulncheck ./...
	@which gosec > /dev/null && gosec ./...

coverage: test
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

bench:
	go test -bench=. -benchmem ./...

clean:
	go clean ./...
	rm -f coverage.out coverage.html

# Development targets
dev:
	go run ./cmd/cobaltdb-server

migrate:
	go run ./cmd/cobaltdb-migrate

cli:
	go run ./cmd/cobaltdb-cli
```

---

### 9.3 GitHub Actions

**Dosya:** `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.24'
      
      - name: Cache Go modules
        uses: actions/cache@v4
        with:
          path: ~/go/pkg/mod
          key: ${{ runner.os }}-go-${{ hashFiles('**/go.sum') }}
      
      - name: Download dependencies
        run: go mod download
      
      - name: Run gofmt
        run: |
          FILES=$(gofmt -l .)
          if [ -n "$FILES" ]; then
            echo "Files need formatting:"
            echo "$FILES"
            exit 1
          fi
      
      - name: Run go vet
        run: go vet ./...
      
      - name: Run tests
        run: go test -v -race -coverprofile=coverage.out ./...
      
      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          file: ./coverage.out

  security:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.24'
      
      - name: Run Gosec Security Scanner
        uses: securego/gosec@master
        with:
          args: ./...
      
      - name: Run govulncheck
        uses: golang/govulncheck-action@v1

  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Go
        uses: actions/setup-go@v5
        with:
          go-version: '1.24'
      
      - name: golangci-lint
        uses: golangci/golangci-lint-action@v4
        with:
          version: latest
```

---

## 10. Quick Reference

### Hızlı Düzeltme Komutları

```bash
# Format all files
gofmt -w .

# Run linter
go vet ./...

# Run tests with coverage
go test -coverprofile=coverage.out ./...

# Check for race conditions (requires CGO)
CGO_ENABLED=1 go test -race ./...

# Security scan
govulncheck ./...
gosec ./...

# Dead code detection
staticcheck ./...

# Find unused code
unused ./...

# Update dependencies
go mod tidy
go mod verify
```

### Öncelikli Düzeltilecek Dosyalar

1. `pkg/catalog/catalog_core.go` - Type assertions, encoding
2. `pkg/catalog/catalog_insert.go` - UNIQUE checks, JSON encoding
3. `pkg/catalog/catalog_update.go` - UNIQUE/FK checks
4. `pkg/protocol/mysql.go` - WaitGroup, context
5. `pkg/storage/encryption.go` - Error wrapping
6. `pkg/auth/auth.go` - Session token, cleanup goroutine
7. `pkg/server/tls.go` - InsecureSkipVerify, error wrapping

---

## Sonuç

Bu dokümandaki düzeltmelerin uygulanması:

- **Phase 1 (Kritik):** ~80 saat - Type safety, concurrency, error handling
- **Phase 2 (Performans):** ~120 saat - Index, encoding, caching
- **Phase 3 (Kalite):** ~60 saat - Refactoring, documentation
- **Phase 4 (Güvenlik):** ~40 saat - Auth, encryption, hardening

**Toplam:** ~300 saat (yaklaşık 8 hafta)

---

*Doküman Tarihi: 10 Mart 2026*
*Go Version: 1.24.0*
