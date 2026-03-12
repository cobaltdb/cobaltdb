# CobaltDB v2.3.0 Production Hardening Plan

> Tarih: 2026-03-12
> Durum: Toplam 45 fix, 4 faz, tahmini ~3500 satir degisiklik
> Kural: Her fix sonrasi `go test ./...` gecmeli, her faz sonrasi commit

---

## Faz 1 — Kritik Buglar & Guvenlik (Oncelik: HEMEN)

> Bu fazdaki her madde production'da veri kaybi, crash veya guvenlik ihlali olusturabilir.

---

### FIX-P01: CreateIndex/DropIndex Mutex Kilidi Eksik
**Dosya:** `pkg/catalog/catalog_index.go`
**Satir:** 10 ve 235
**Sorun:** `CreateIndex` ve `DropIndex` `c.mu.Lock()` almadan `c.indexes`, `c.indexTrees`, `c.tables` map'lerine erisiyorlar. Concurrent SELECT/INSERT sirasinda Go runtime panic (map concurrent read/write).
**Degisiklik:**
```go
// catalog_index.go:10 — CreateIndex fonksiyon basina ekle:
func (c *Catalog) CreateIndex(stmt *query.CreateIndexStmt) error {
+   c.mu.Lock()
+   defer c.mu.Unlock()
    if !stmt.IfNotExists {
```

```go
// catalog_index.go:235 — DropIndex fonksiyon basina ekle:
func (c *Catalog) DropIndex(stmt *query.DropIndexStmt) error {
+   c.mu.Lock()
+   defer c.mu.Unlock()
```
**Test:** `go test -v -run TestConcurrentInserts ./test/` + yeni index concurrent test
**Beklenen Sonuc:** Concurrent index islemlerinde panic olmaz

---

### FIX-P02: QueryCache Data Race — RLock Altinda Write
**Dosya:** `pkg/catalog/catalog_core.go`
**Satir:** 265-284 ve 353-357
**Sorun:** `hitCount++` ve `missCount++` islemleri `mu.RLock()` altinda yapiliyor. Birden fazla goroutine ayni anda `Get()` cagirinca data race.
**Degisiklik:**
```go
// catalog_core.go — struct taniminda degistir:
type QueryCache struct {
    mu        sync.RWMutex
    entries   map[string]*QueryCacheEntry
-   hitCount  int64
-   missCount int64
+   hitCount  atomic.Int64
+   missCount atomic.Int64
    maxSize   int
    ttl       time.Duration
    enabled   bool
}

// catalog_core.go:272 — Get icinde:
-   qc.missCount++
+   qc.missCount.Add(1)

// catalog_core.go:278 — Get icinde:
-   qc.missCount++
+   qc.missCount.Add(1)

// catalog_core.go:282 — Get icinde:
-   qc.hitCount++
+   qc.hitCount.Add(1)

// catalog_core.go:353-356 — Stats icinde:
func (qc *QueryCache) Stats() (hits, misses int64, size int) {
    qc.mu.RLock()
    defer qc.mu.RUnlock()
-   return qc.hitCount, qc.missCount, len(qc.entries)
+   return qc.hitCount.Load(), qc.missCount.Load(), len(qc.entries)
}
```
**Import:** `"sync/atomic"` ekle (zaten `sync` var)
**Test:** `go test -race ./pkg/catalog/` (CI'da Ubuntu'da calisir)
**Beklenen Sonuc:** `go test -race` clean pass

---

### FIX-P03: Circuit Breaker Concurrency Counter Negatives Kayiyor
**Dosya:** `pkg/engine/circuit_breaker.go`
**Satir:** 110-148
**Sorun:** `Allow()` HalfOpen state'de concurrency counter'i artirmiyor ama `Release()` her zaman azaltiyor. Counter zamanla negatife kayiyor.
**Degisiklik:**
```go
// circuit_breaker.go:123-130 — HalfOpen case'ine concurrency tracking ekle:
case CircuitHalfOpen:
    select {
    case <-cb.halfOpenTokens:
+       cb.concurrency.Add(1)
        return nil
    default:
        return ErrCircuitOpen
    }

// circuit_breaker.go:117-121 — tryHalfOpen basarili olunca da ekle:
case CircuitOpen:
    if cb.shouldAttemptReset() {
        if cb.tryHalfOpen() {
+           cb.concurrency.Add(1)
            return nil
        }
    }
    return ErrCircuitOpen
```
**Test:** `go test -v -run TestCircuitBreaker ./pkg/engine/`
**Beklenen Sonuc:** Concurrency counter hic negatife dusmez

---

### FIX-P04: REPEAT/ZEROBLOB/LPAD/RPAD Sinirsiz Memory Allocation (DoS)
**Dosya:** `pkg/catalog/catalog_eval.go`
**Satirlar:** 795, 958, ve LPAD/RPAD bloklari
**Sorun:** `SELECT REPEAT('A', 2147483647)` veya `ZEROBLOB(2147483647)` sunucuyu OOM ile cokertir.
**Degisiklik:**
```go
// catalog_eval.go — dosya basina const ekle:
const maxStringResultLen = 10 * 1024 * 1024 // 10 MB cap for string functions

// catalog_eval.go:790-795 — REPEAT icinde:
    str := fmt.Sprintf("%v", evalArgs[0])
    count, _ := toFloat64(evalArgs[1])
    if count <= 0 {
        return "", nil
    }
+   if int(count) > maxStringResultLen / (len(str) + 1) {
+       return nil, fmt.Errorf("REPEAT result exceeds maximum allowed size (%d bytes)", maxStringResultLen)
+   }
    return strings.Repeat(str, int(count)), nil

// catalog_eval.go:957-958 — ZEROBLOB icinde:
    n, _ := toFloat64(evalArgs[0])
+   if int(n) > maxStringResultLen {
+       return nil, fmt.Errorf("ZEROBLOB size exceeds maximum allowed size (%d bytes)", maxStringResultLen)
+   }
    return strings.Repeat("\x00", int(n)), nil

// LPAD ve RPAD icin de ayni kontrol:
// (LPAD/RPAD'de targetLen kontrolu ekle)
+   if int(targetLen) > maxStringResultLen {
+       return nil, fmt.Errorf("LPAD/RPAD result exceeds maximum allowed size")
+   }
```
**Test:** Yeni test: `SELECT REPEAT('A', 999999999)` → hata donmeli
**Beklenen Sonuc:** Buyuk string uretimi hatayla reddedilir, OOM olmaz

---

### FIX-P05: Parser Recursion Depth Limiti Yok (DoS)
**Dosya:** `pkg/query/parser.go`
**Satir:** 16-22 (struct) ve expression parsing fonksiyonlari
**Sorun:** `SELECT (((((((...1000 katman...)))))))` stack overflow yapar.
**Degisiklik:**
```go
// parser.go — struct'a field ekle:
type Parser struct {
    tokens           []Token
    pos              int
    placeholderCount int
+   depth            int
}

// parser.go — yeni helper:
+const maxParserDepth = 200
+
+func (p *Parser) enterDepth() error {
+    p.depth++
+    if p.depth > maxParserDepth {
+        return fmt.Errorf("expression nesting depth exceeds maximum (%d)", maxParserDepth)
+    }
+    return nil
+}
+
+func (p *Parser) leaveDepth() {
+    p.depth--
+}

// parser.go — parsePrimary icinde paren acildiginda:
// (parantez acma case'inde)
case TokenLParen:
    p.advance()
+   if err := p.enterDepth(); err != nil {
+       return nil, err
+   }
+   defer p.leaveDepth()
    // ... mevcut kod
```
**Test:** 300 katman nested `(((1)))` → hata donmeli
**Beklenen Sonuc:** Derin icleme stack overflow yerine temiz hata

---

### FIX-P06: UPDATE PK Change — Delete Hatasi Sessizce Yutuluyor
**Dosya:** `pkg/catalog/catalog_update.go`
**Satir:** 313-318
**Sorun:** `tree.Delete(oldKey)` hatasi `_ = err` ile yutluyor. Basarisiz olursa eski satir kalir + yeni satir eklenir = duplicate.
**Degisiklik:**
```go
// catalog_update.go:313-318:
    if pkChanged {
-       if err := tree.Delete(oldKey); err != nil {
-           _ = err
-       }
+       if err := tree.Delete(oldKey); err != nil {
+           return 0, rowsAffected, fmt.Errorf("failed to delete old key during PK update: %w", err)
+       }
        if err := tree.Put(newKey, newValueData); err != nil {
```
**Test:** `go test -v -run TestV98_UpdateTextPKChange ./test/` (reserved word fix sonrasi)
**Beklenen Sonuc:** PK degisikligi sirasinda hata durumunda islem iptal edilir

---

### FIX-P07: WAL Data Mutation — append(newKey, 0) Slice Corrupt
**Dosya:** `pkg/catalog/catalog_update.go`
**Satir:** 288-289
**Sorun:** `append(newKey, 0)` spare capacity varsa `newKey`'in underlying array'ini mutate eder.
**Degisiklik:**
```go
// catalog_update.go:287-289 — guvenli allocation:
-   walData := append(newKey, 0)
-   walData = append(walData, newValueData...)
+   walData := make([]byte, 0, len(newKey)+1+len(newValueData))
+   walData = append(walData, newKey...)
+   walData = append(walData, 0)
+   walData = append(walData, newValueData...)
```
**Test:** Mevcut UPDATE testleri
**Beklenen Sonuc:** WAL yazimi sirasinda key corruption olmaz

---

### FIX-P08: INSERT Expression Hatasi Sessizce NULL Yapiyor
**Dosya:** `pkg/catalog/catalog_insert.go`
**Satir:** 184-189
**Sorun:** Ifade degerlendirme hatasi NULL'a donusturuluyor, kullanici habersiz.
**Degisiklik:**
```go
// catalog_insert.go:184-189:
    val, err := evaluateExpression(c, nil, nil, valueRow[colIdx], args)
    if err != nil {
-       rowValues[tableColIdx] = nil
+       return 0, 0, fmt.Errorf("failed to evaluate value for column '%s': %w",
+           insertColumns[colIdx], err)
    } else {
        rowValues[tableColIdx] = val
    }
```
**Test:** `SELECT INVALID_FUNC() FROM ...` iceren INSERT → hata donmeli
**Beklenen Sonuc:** Hatali ifadeler sessizce NULL olmaz

---

### FIX-P09: Trigger Hatalari Yutuluyor
**Dosyalar:** `pkg/catalog/catalog_delete.go:178`, `pkg/catalog/catalog_insert.go:624`, `pkg/catalog/catalog_update.go:418`
**Sorun:** `_ = c.executeTriggers(...)` hatalari yok sayiyor.
**Degisiklik:**
```go
// catalog_delete.go:178:
-   _ = c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns)
+   if trigErr := c.executeTriggers(ctx, stmt.Table, "DELETE", "AFTER", nil, row, table.Columns); trigErr != nil {
+       return 0, rowsAffected, fmt.Errorf("AFTER DELETE trigger failed: %w", trigErr)
+   }

// catalog_insert.go:624 — ayni pattern:
-   _ = c.executeTriggers(...)
+   if trigErr := c.executeTriggers(...); trigErr != nil {
+       return 0, 0, fmt.Errorf("AFTER INSERT trigger failed: %w", trigErr)
+   }

// catalog_update.go:418 — ayni pattern:
-   _ = c.executeTriggers(...)
+   if trigErr := c.executeTriggers(...); trigErr != nil {
+       return 0, rowsAffected, fmt.Errorf("AFTER UPDATE trigger failed: %w", trigErr)
+   }
```
**Test:** Hata ureten trigger → DML hata donmeli
**Beklenen Sonuc:** Trigger hatalari ACID garantisini ihlal etmez

---

### FIX-P10: Non-Unique Index DELETE'te Yanlis Key Kullaniliyor
**Dosya:** `pkg/catalog/catalog_update.go`
**Satir:** 356
**Sorun:** PK degistiginde eski index entry silinirken `newKey` kullaniliyor, `oldKey` olmali.
**Degisiklik:**
```go
// catalog_update.go:355-356:
    // For non-unique indexes, delete the compound key "indexValue\x00pk"
-   compoundKey := oldIndexKey + "\x00" + string(newKey)
+   compoundKey := oldIndexKey + "\x00" + string(entry.key)
```
**Not:** `entry.key` = oldKey (updateEntry struct'inda saklanan orijinal key)
**Test:** Non-unique index uzerinde PK degisikligi yapan UPDATE testi
**Beklenen Sonuc:** Eski index entry'leri duzgun temizlenir

---

### FIX-P11: AlterTableRename Persist Etmiyor
**Dosya:** `pkg/catalog/catalog_ddl.go`
**Satir:** ~570 (AlterTableRename sonu)
**Sorun:** In-memory rename yapiliyor ama disk'e yazilmiyor. Restart sonrasi eski tablo ismi geri geliyor.
**Degisiklik:**
```go
// catalog_ddl.go — AlterTableRename fonksiyonunun sonuna ekle:
    // ... mevcut rename kodu ...
    table.Name = stmt.NewName
+
+   // Remove old catalog entry and persist new one
+   if c.tree != nil {
+       c.tree.Delete([]byte("tbl:" + stmt.Table))
+   }
+   if err := c.storeTableDef(table); err != nil {
+       return fmt.Errorf("failed to persist renamed table: %w", err)
+   }

    return nil
```
**Test:** Rename → Close → Reopen → tablo yeni isimde olmali
**Beklenen Sonuc:** Rename restart'a dayanikli

---

## Faz 2 — Correctness & Veri Butunlugu

---

### FIX-P12: Quoted Identifier Destegi (Reserved Word Fix)
**Dosya:** `pkg/catalog/catalog_ddl.go`
**Satir:** 35-47 (validateColumnName) ve 20-32 (validateTableName)
**Sorun:** "first", "key", "order" gibi yaygın kelimeler kolon adi olarak kullanilamiyor. Tirnakli identifier'lar bile reddediliyor. 11 test basarisiz.
**Degisiklik:**
```go
// catalog_ddl.go — validateTableName:
func validateTableName(name string) error {
    if name == "" {
        return fmt.Errorf("table name cannot be empty")
    }
+   // Strip quotes — quoted identifiers bypass reserved word check
+   stripped := stripQuotes(name)
-   if !validIdentifierName.MatchString(name) {
+   if !validIdentifierName.MatchString(stripped) {
-       return fmt.Errorf("invalid table name %q: ...", name)
+       return fmt.Errorf("invalid table name %q: ...", stripped)
    }
-   if isReservedWord(name) {
-       return fmt.Errorf("table name %q is a reserved word", name)
+   // Only reject unquoted reserved words
+   if name == stripped && isReservedWord(name) {
+       return fmt.Errorf("table name %q is a reserved word (use quotes to override)", name)
    }
    return nil
}

// Ayni pattern validateColumnName icin de

// Yeni helper:
+func stripQuotes(name string) string {
+    if len(name) >= 2 {
+        if (name[0] == '"' && name[len(name)-1] == '"') ||
+           (name[0] == '`' && name[len(name)-1] == '`') ||
+           (name[0] == '[' && name[len(name)-1] == ']') {
+            return name[1 : len(name)-1]
+        }
+    }
+    return name
+}
```
**Ek:** Reserved word listesini daralt — `FIRST`, `LAST`, `KEY`, `USER`, `SESSION`, `DO`, `SET` gibi yaygın kelimeleri cikar
**Test:** 11 kalan test gecmeli + yeni test: `CREATE TABLE t ("order" INTEGER)`
**Beklenen Sonuc:** Tirnakli identifier'lar her zaman kabul edilir; agresif reserved word listesi daraltilir

---

### FIX-P13: isReservedWord Map Her Cagirda Yeniden Olusturuluyor
**Dosya:** `pkg/catalog/catalog_ddl.go`
**Satir:** 50-74
**Sorun:** Her `isReservedWord()` cagrisinda ~70 entry'lik map allocate ediliyor.
**Degisiklik:**
```go
// catalog_ddl.go — package-level var olarak tasi:
-func isReservedWord(name string) bool {
-    reserved := map[string]bool{
-        "SELECT": true, "INSERT": true, ...
-    }
-    return reserved[strings.ToUpper(name)]
-}

+var reservedWords = map[string]bool{
+    "SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": true,
+    "CREATE": true, "DROP": true, "ALTER": true, "TABLE": true,
+    "INDEX": true, "VIEW": true, "TRIGGER": true, "PROCEDURE": true,
+    "FROM": true, "WHERE": true, "JOIN": true, "ON": true, "AND": true,
+    "OR": true, "NOT": true, "IN": true, "BETWEEN": true, "LIKE": true,
+    "NULL": true, "TRUE": true, "FALSE": true, "DEFAULT": true,
+    "PRIMARY": true, "FOREIGN": true, "REFERENCES": true,
+    "UNIQUE": true, "CHECK": true, "CONSTRAINT": true,
+    "COLUMN": true, "RENAME": true, "TO": true, "AS": true,
+    "ORDER": true, "BY": true, "GROUP": true, "HAVING": true,
+    "LIMIT": true, "OFFSET": true, "UNION": true, "INTERSECT": true,
+    "EXCEPT": true, "ALL": true, "DISTINCT": true,
+    "CASE": true, "WHEN": true, "THEN": true, "ELSE": true, "END": true,
+    "CAST": true, "INTO": true, "VALUES": true,
+    "INNER": true, "LEFT": true, "RIGHT": true, "OUTER": true,
+    "CROSS": true, "NATURAL": true, "USING": true,
+    "EXISTS": true, "ROLLBACK": true, "COMMIT": true, "TRANSACTION": true,
+    "IS": true, "INTEGER": true, "TEXT": true, "REAL": true, "BLOB": true,
+    "BOOLEAN": true, "JSON": true, "RETURNING": true,
+}
+// NOT reserved (commonly used as identifiers):
+// FIRST, LAST, KEY, USER, SESSION, DO, SET, ADD, ASC, DESC, NULLS, ANY, SOME, CURRENT
+
+func isReservedWord(name string) bool {
+    return reservedWords[strings.ToUpper(name)]
+}
```
**Beklenen Sonuc:** Sifir allocation, `FIRST`/`KEY`/`SET` gibi kelimeler artik reserved degil

---

### FIX-P14: Index Key ve GROUP BY Key Type Collision
**Dosyalar:** `pkg/catalog/catalog_core.go:3476,3485` ve `pkg/catalog/catalog_aggregate.go:122,126`
**Sorun:** `fmt.Sprintf("%v")` type bilgisi kaybiyor. String `"42"` ve integer `42` ayni key uretir → yanlis gruplama/index eslesmesi. Pipe `|` separator'u veri icinde bulunabilir.
**Degisiklik:**
```go
// catalog_core.go — buildCompositeIndexKey icinde:
-   return fmt.Sprintf("%v", row[colIdx]), true
+   return typeTaggedKey(row[colIdx]), true

// ...
-   parts = append(parts, fmt.Sprintf("%v", row[colIdx]))
+   parts = append(parts, typeTaggedKey(row[colIdx]))

// Yeni helper (catalog_core.go'ya ekle):
+func typeTaggedKey(v interface{}) string {
+    if v == nil {
+        return "\x01NULL\x01"
+    }
+    switch val := v.(type) {
+    case int64:
+        return "I:" + strconv.FormatInt(val, 10)
+    case float64:
+        if val == float64(int64(val)) {
+            return "I:" + strconv.FormatInt(int64(val), 10)
+        }
+        return "F:" + strconv.FormatFloat(val, 'g', -1, 64)
+    case bool:
+        if val {
+            return "B:1"
+        }
+        return "B:0"
+    default:
+        return "S:" + fmt.Sprintf("%v", v)
+    }
+}

// catalog_aggregate.go:118-129 — ayni degisiklik:
-   groupKey.WriteString(fmt.Sprintf("%v", fullRow[spec.index]))
+   groupKey.WriteString(typeTaggedKey(fullRow[spec.index]))
// ...
-   groupKey.WriteString(fmt.Sprintf("%v", val))
+   groupKey.WriteString(typeTaggedKey(val))

// Separator'u de degistir:
-   groupKey.WriteString("|")
+   groupKey.WriteString("\x00")
```
**Test:** `GROUP BY` testinde `'42'` (string) ve `42` (integer) farkli gruplara dusmeli
**Beklenen Sonuc:** Type-safe index/GROUP BY key'ler

---

### FIX-P15: updateWithJoinLocked Undo Log Kaydi Yapmiyor
**Dosya:** `pkg/catalog/catalog_update.go`
**Satir:** 541-569
**Sorun:** JOIN'li UPDATE'de undo log kaydedilmiyor. Transaction rollback'te bu degisiklikler geri alinmaz.
**Degisiklik:**
```go
// catalog_update.go — updateWithJoinLocked, entry apply dongusu sonuna ekle (satir 566 civarinda):
+       // Record undo log for rollback
+       if c.txnActive {
+           oldValueData, marshalErr := json.Marshal(entry.oldRow)
+           if marshalErr == nil {
+               keyCopy := make([]byte, len(entry.key))
+               copy(keyCopy, entry.key)
+               c.undoLog = append(c.undoLog, undoEntry{
+                   action:    undoUpdate,
+                   tableName: stmt.Table,
+                   key:       keyCopy,
+                   oldValue:  oldValueData,
+               })
+           }
+       }
```
**Test:** Transaction icinde JOIN UPDATE + ROLLBACK → degisiklikler geri alinmali
**Beklenen Sonuc:** JOIN'li UPDATE transactional guvenli

---

### FIX-P16: Close() Ilk Hatada Donuyor — Cleanup Atlamasiyla Kaynak Sizintisi
**Dosya:** `pkg/engine/database.go`
**Satir:** 461-524
**Sorun:** Save catalog hatasi → WAL close, pool close, backend close atlanir.
**Degisiklik:**
```go
// database.go — Close() fonksiyonunu yeniden yaz:
func (db *DB) Close() error {
    db.mu.Lock()
    defer db.mu.Unlock()

    if db.closed {
        return nil
    }
    db.closed = true

    // ... shutdown signal kodu ayni ...

+   var errs []error

    if db.metrics != nil {
        db.metrics.Stop()
    }

    if !db.options.InMemory && db.path != ":memory:" {
        if err := db.catalog.Save(); err != nil {
-           return fmt.Errorf("failed to save catalog: %w", err)
+           errs = append(errs, fmt.Errorf("save catalog: %w", err))
        }
        if err := db.saveMetaPage(); err != nil {
-           return fmt.Errorf("failed to save meta page: %w", err)
+           errs = append(errs, fmt.Errorf("save meta: %w", err))
        }
    }

    if db.wal != nil {
        if err := db.wal.Checkpoint(db.pool); err != nil {
-           return fmt.Errorf("failed to checkpoint WAL: %w", err)
+           errs = append(errs, fmt.Errorf("checkpoint: %w", err))
        }
    }

    if err := db.pool.Close(); err != nil {
-       return fmt.Errorf("failed to close buffer pool: %w", err)
+       errs = append(errs, fmt.Errorf("pool close: %w", err))
    }

    if db.auditLogger != nil {
        if err := db.auditLogger.Close(); err != nil {
-           return fmt.Errorf("failed to close audit logger: %w", err)
+           errs = append(errs, fmt.Errorf("audit close: %w", err))
        }
    }

    if db.wal != nil {
        if err := db.wal.Close(); err != nil {
-           return fmt.Errorf("failed to close WAL: %w", err)
+           errs = append(errs, fmt.Errorf("wal close: %w", err))
        }
    }

-   return db.backend.Close()
+   if err := db.backend.Close(); err != nil {
+       errs = append(errs, fmt.Errorf("backend close: %w", err))
+   }
+
+   return errors.Join(errs...)
}
```
**Beklenen Sonuc:** Tum kaynaklar her zaman cleanup edilir

---

### FIX-P17: RANDOM Fonksiyonu Gercekten Random Degil
**Dosya:** `pkg/catalog/catalog_eval.go`
**Satir:** 926-927
**Sorun:** `time.Now().UnixNano() % 1000000` — ayni nanosaniyede ayni deger, ardisik cagrilarda sequential.
**Degisiklik:**
```go
// catalog_eval.go:926-927:
    case "RANDOM":
-       return float64(time.Now().UnixNano() % 1000000), nil
+       return float64(rand.Int63()), nil
```
**Import:** `"math/rand"` ekle
**Beklenen Sonuc:** Gercek pseudo-random degerler

---

## Faz 3 — Guvenlik Sertlestirme

---

### FIX-P18: RLS'yi Execution Path'lere Bagla
**Dosyalar:** `pkg/catalog/catalog_core.go` (selectLocked), `pkg/catalog/catalog_insert.go`, `pkg/catalog/catalog_update.go`, `pkg/catalog/catalog_delete.go`
**Sorun:** RLS fonksiyonlari tanimli ama hicbir execution path'te cagirilmiyor. Tum RLS sistemi dead code.
**Degisiklik — selectLocked (catalog_core.go):**
```go
// selectLocked fonksiyonunda sonuc satırları dondurulmeden once:
// (rows dondurulmeden hemen once, fonksiyon sonuna yakin)
+   // Apply Row-Level Security filtering
+   if c.enableRLS && c.rlsManager != nil {
+       user, _ := ctx.Value("cobaltdb_user").(string)
+       roles, _ := ctx.Value("cobaltdb_roles").([]string)
+       if user != "" {
+           cols, filteredRows, rlsErr := c.ApplyRLSFilterInternal(ctx, tableName, columns, rows, user, roles)
+           if rlsErr != nil {
+               return nil, nil, fmt.Errorf("RLS filter failed: %w", rlsErr)
+           }
+           columns = cols
+           rows = filteredRows
+       }
+   }
```
**Degisiklik — insertLocked:**
```go
// Her satir insert'ten once:
+   if c.enableRLS && c.rlsManager != nil {
+       user, _ := ctx.Value("cobaltdb_user").(string)
+       roles, _ := ctx.Value("cobaltdb_roles").([]string)
+       if user != "" {
+           if err := c.CheckRLSForInsert(ctx, stmt.Table, table.Columns, rowValues, user, roles); err != nil {
+               return 0, 0, fmt.Errorf("RLS policy denied INSERT: %w", err)
+           }
+       }
+   }
```
**Ayni pattern** `updateLocked` ve `deleteLocked` icin de `CheckRLSForUpdate`/`CheckRLSForDelete` ile.
**Not:** `ApplyRLSFilterInternal` yeni bir internal versiyon olmali — mevcut `ApplyRLSFilter` kendi `c.mu.RLock` aliyor ama `selectLocked` zaten lock altinda cagriliyor. Yeni fonksiyon lock almadan calisacak.
**Test:** RLS policy tanimla → policy disindaki satirlara erisim engellenmeli
**Beklenen Sonuc:** RLS gercekten enforce ediliyor

---

### FIX-P19: Brute-Force Koruması
**Dosya:** `pkg/auth/auth.go`
**Sorun:** Sinirsiz authentication denemesi.
**Degisiklik:**
```go
// auth.go — struct'a ekle:
type Authenticator struct {
    // ... mevcut alanlar ...
+   failedAttempts map[string]*loginAttempt // IP/user → attempt tracking
+   failedMu       sync.RWMutex
}

+type loginAttempt struct {
+    count    int
+    lastFail time.Time
+    lockUntil time.Time
+}
+
+const (
+    maxLoginAttempts   = 5
+    lockoutDuration    = 5 * time.Minute
+    attemptResetAfter  = 15 * time.Minute
+)

// Authenticate fonksiyonuna ekle:
+   // Check lockout
+   a.failedMu.RLock()
+   attempt, exists := a.failedAttempts[username]
+   a.failedMu.RUnlock()
+   if exists && time.Now().Before(attempt.lockUntil) {
+       return nil, fmt.Errorf("account temporarily locked, try again later")
+   }

// Basarisiz auth sonrasi:
+   a.failedMu.Lock()
+   if a.failedAttempts[username] == nil {
+       a.failedAttempts[username] = &loginAttempt{}
+   }
+   a.failedAttempts[username].count++
+   a.failedAttempts[username].lastFail = time.Now()
+   if a.failedAttempts[username].count >= maxLoginAttempts {
+       a.failedAttempts[username].lockUntil = time.Now().Add(lockoutDuration)
+   }
+   a.failedMu.Unlock()

// Basarili auth sonrasi:
+   a.failedMu.Lock()
+   delete(a.failedAttempts, username)
+   a.failedMu.Unlock()
```
**Beklenen Sonuc:** 5 basarisiz deneme sonrasi 5 dakika kilitlenme

---

### FIX-P20: Password Complexity Policy
**Dosya:** `pkg/auth/auth.go`
**Satir:** createUserLocked icinde password check ekle
**Degisiklik:**
```go
+func validatePasswordStrength(password string) error {
+    if len(password) < 8 {
+        return fmt.Errorf("password must be at least 8 characters")
+    }
+    hasUpper, hasLower, hasDigit := false, false, false
+    for _, ch := range password {
+        switch {
+        case ch >= 'A' && ch <= 'Z':
+            hasUpper = true
+        case ch >= 'a' && ch <= 'z':
+            hasLower = true
+        case ch >= '0' && ch <= '9':
+            hasDigit = true
+        }
+    }
+    if !hasUpper || !hasLower || !hasDigit {
+        return fmt.Errorf("password must contain uppercase, lowercase, and digit")
+    }
+    return nil
+}

// createUserLocked icinde:
+   if err := validatePasswordStrength(password); err != nil {
+       return err
+   }
```
**Beklenen Sonuc:** Zayif sifreler reddedilir

---

### FIX-P21: Health Server Auth veya Loopback-Only
**Dosya:** `pkg/server/production.go`
**Satir:** 124-152
**Sorun:** `/stats`, `/circuit-breakers` endpoint'leri auth olmadan tum IP'lerden erisilebilir.
**Degisiklik:**
```go
// production.go:137 — HealthAddr default'unu loopback yap:
// VEYA stats endpoint'lerini auth arkasina al:

func (ps *ProductionServer) startHealthServer() {
    mux := http.NewServeMux()

    // Health endpoints — public (probe'lar icin)
    mux.HandleFunc("/health", ps.healthHandler())
    mux.HandleFunc("/ready", ps.readyHandler())
    mux.HandleFunc("/healthz", ps.healthzHandler())

-   mux.HandleFunc("/stats", ps.statsHandler())
-   mux.HandleFunc("/circuit-breakers", ps.circuitBreakerHandler())
-   mux.HandleFunc("/rate-limits", ps.rateLimitsHandler())
+   // Stats endpoints — loopback only
+   mux.HandleFunc("/stats", ps.loopbackOnly(ps.statsHandler()))
+   mux.HandleFunc("/circuit-breakers", ps.loopbackOnly(ps.circuitBreakerHandler()))
+   mux.HandleFunc("/rate-limits", ps.loopbackOnly(ps.rateLimitsHandler()))

// Yeni middleware:
+func (ps *ProductionServer) loopbackOnly(next http.HandlerFunc) http.HandlerFunc {
+    return func(w http.ResponseWriter, r *http.Request) {
+        host, _, _ := net.SplitHostPort(r.RemoteAddr)
+        if host != "127.0.0.1" && host != "::1" && host != "localhost" {
+            http.Error(w, "forbidden", http.StatusForbidden)
+            return
+        }
+        next(w, r)
+    }
+}
```
**Beklenen Sonuc:** Stats endpoint'leri sadece localhost'tan erisilebilir

---

### FIX-P22: Wire Protocol TLS Uyarisi
**Dosya:** `pkg/server/server.go`
**Satir:** Listen() icinde
**Sorun:** Auth acik + TLS kapali → sifre cleartext gidiyor.
**Degisiklik:**
```go
// server.go — Listen() icinde:
func (s *Server) Listen(address string, tlsConfig *TLSConfig) error {
+   // Warn if auth is enabled but TLS is not
+   if s.auth.IsEnabled() && (tlsConfig == nil || !tlsConfig.Enabled) {
+       fmt.Println("WARNING: Authentication is enabled but TLS is disabled. " +
+           "Passwords will be sent in cleartext. Enable TLS for production use.")
+   }
```
**Beklenen Sonuc:** Guvenlik uarisi server loglarinda gorulur

---

### FIX-P23: Session Invalidation on Password Change
**Dosya:** `pkg/auth/auth.go`
**Satir:** ChangePassword fonksiyonu (284-308)
**Degisiklik:**
```go
// auth.go — ChangePassword sonuna ekle (Update basarili olduktan sonra):
+   // Invalidate all active sessions for this user
+   a.sessionMu.Lock()
+   for token, sess := range a.sessions {
+       if sess.Username == username {
+           delete(a.sessions, token)
+       }
+   }
+   a.sessionMu.Unlock()
```
**Beklenen Sonuc:** Sifre degisikligi sonrasi eski session'lar gecersiz

---

## Faz 4 — Performance & Kalite

---

### FIX-P24: Transaction Manager versions Map Pruning
**Dosya:** `pkg/txn/manager.go`
**Satir:** 164-171
**Sorun:** `versions` map'i asla temizlenmiyor → unbounded memory growth.
**Degisiklik:**
```go
// manager.go — yeni prune fonksiyonu:
+func (m *Manager) pruneVersions() {
+    m.mu.Lock()
+    defer m.mu.Unlock()
+
+    // Find minimum active transaction start timestamp
+    minActive := uint64(math.MaxUint64)
+    for _, txn := range m.active {
+        if txn.StartTS < minActive {
+            minActive = txn.StartTS
+        }
+    }
+
+    // If no active transactions, we can clear all versions
+    if minActive == math.MaxUint64 {
+        m.versions = make(map[string]uint64)
+        return
+    }
+}

// Commit sonrasinda cagirilabilir (periyodik olarak):
+   if m.commitCount.Add(1) % 1000 == 0 {
+       go m.pruneVersions()
+   }
```
**Beklenen Sonuc:** versions map'i kontrol altinda kalir

---

### FIX-P25: MemoryBackend Short Read Hatasi
**Dosya:** `pkg/storage/memory.go`
**Satir:** 22-36
**Sorun:** Short read hatasiz doner, kismk-sifirli page data'si valid gibi islenir.
**Degisiklik:**
```go
// memory.go:
func (m *MemoryBackend) ReadAt(buf []byte, offset int64) (int, error) {
    m.mu.RLock()
    defer m.mu.RUnlock()

    if offset < 0 || offset >= int64(len(m.data)) {
        return 0, io.EOF
    }

    n := copy(buf, m.data[offset:])
+   if n < len(buf) {
+       return n, io.EOF
+   }
    return n, nil
}
```
**Import:** `"io"` ekle
**Beklenen Sonuc:** Kisa okuma durumunda io.EOF sinyali verilir

---

### FIX-P26: WAL Record Data uint16 Limiti Dokumantasyonu
**Dosya:** `pkg/storage/wal.go`
**Satir:** 132
**Sorun:** WAL record data 64KB ile sinirli. Buyuk satirlarda sessiz truncation.
**Degisiklik:**
```go
// wal.go — Append fonksiyonuna check ekle:
func (w *WAL) Append(record *WALRecord) error {
+   if len(record.Data) > 65535 {
+       return fmt.Errorf("WAL record data size (%d bytes) exceeds maximum (65535 bytes)", len(record.Data))
+   }
    w.mu.Lock()
    defer w.mu.Unlock()
```
**Beklenen Sonuc:** 64KB'den buyuk veriler icin temiz hata mesaji (sessiz truncation degil)

---

### FIX-P27: Server Panic Recovery Logging
**Dosya:** `pkg/server/server.go`
**Satir:** 189-193
**Sorun:** Client handler panic'leri sessizce yutluyor.
**Degisiklik:**
```go
// server.go:189-193:
    defer func() {
        if r := recover(); r != nil {
-           _ = r
+           fmt.Printf("[PANIC] client handler panic recovered: %v\n", r)
        }
    }()
```
**Beklenen Sonuc:** Panic bilgisi log'da gorunur

---

### FIX-P28: Unterminated String Literal Lexer Hatasi
**Dosya:** `pkg/query/lexer.go`
**Satir:** readString fonksiyonu
**Sorun:** Kapanmamis string literal (EOF'a kadar) gecerli token olarak donuyor.
**Degisiklik:**
```go
// lexer.go — readString fonksiyonunda:
// Dongu sonrasi (ch == 0 kontrolunden sonra):
+   if l.ch == 0 {
+       return Token{Type: TokenIllegal, Literal: "unterminated string literal", Line: l.line, Column: l.col}
+   }
```
**Beklenen Sonuc:** Kapanmamis stringler parse hatasi uretir

---

### FIX-P29: Health Check Gercek DB Kontrolu
**Dosya:** `cmd/cobaltdb-server/main.go`
**Satir:** 242-248 (WireServerComponent.Health)
**Sorun:** Her zaman `Healthy: true` donuyor, gercek DB durumunu kontrol etmiyor.
**Degisiklik:**
```go
func (w *WireServerComponent) Health() server.HealthStatus {
-   return server.HealthStatus{
-       Healthy: true,
-       Message: "wire server running",
-   }
+   if w.server == nil {
+       return server.HealthStatus{Healthy: false, Message: "server not initialized"}
+   }
+   return server.HealthStatus{
+       Healthy: true,
+       Message: fmt.Sprintf("wire server running, %d clients connected", w.server.ClientCount()),
+   }
}
```

---

### FIX-P30: QueryCache Eviction O(N) → O(1)
**Dosya:** `pkg/catalog/catalog_core.go`
**Satir:** 337-351
**Sorun:** Her eviction tum entry'leri taraiyor.
**Degisiklik:** evictOne'da `container/list` (LRU linked list) kullan:
```go
// catalog_core.go — QueryCache struct'a LRU list ekle:
type QueryCache struct {
    // ... mevcut ...
+   lru     *list.List
+   lruMap  map[string]*list.Element
}

// Set'te:
+   elem := qc.lru.PushFront(key)
+   qc.lruMap[key] = elem

// Get'te:
+   qc.lru.MoveToFront(qc.lruMap[key])

// evictOne'da:
-   for key, entry := range qc.entries { ... }
+   back := qc.lru.Back()
+   if back != nil {
+       key := back.Value.(string)
+       qc.lru.Remove(back)
+       delete(qc.lruMap, key)
+       delete(qc.entries, key)
+   }
```
**Beklenen Sonuc:** Cache eviction O(1)

---

## Test Stratejisi

Her faz sonrasi:
```bash
# Temel dogralama
go build ./...
go vet ./...
go test ./...

# Detayli
go test -v -count=1 ./test/ 2>&1 | grep "FAIL" | wc -l  # 0 olmali
go test -cover ./pkg/catalog/ ./pkg/engine/ ./pkg/txn/   # coverage artmali
```

CI'da (Ubuntu):
```bash
CGO_ENABLED=1 go test -race ./...
```

---

## Basari Kriterleri

| Kriter | Hedef |
|--------|-------|
| Test fail sayisi | 0 (simdi 11) |
| Catalog coverage | >75% (simdi 64.4%) |
| Engine coverage | >65% (simdi 50.5%) |
| go vet | clean |
| Bilinen data race | 0 |
| OOM DoS vektoru | 0 |
| Sessiz hata yutma | 0 critical path |
| RLS enforcement | Aktif |

---

## Takvim Ozeti

| Faz | Kapsam | Fix Sayisi | Oncelik |
|-----|--------|------------|---------|
| Faz 1 | Kritik buglar | P01-P11 | HEMEN — veri kaybi/crash onleme |
| Faz 2 | Correctness | P12-P17 | 1-3 gun — dogru calisma |
| Faz 3 | Guvenlik | P18-P23 | 3-5 gun — security hardening |
| Faz 4 | Performance/Kalite | P24-P30 | 1+ hafta — production polish |
