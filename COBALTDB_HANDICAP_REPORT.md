# CobaltDB Handikap ve Sınırlama Raporu

**Tarih:** 2026-05-05
**Kapsam:** Kaynak kod ve dokümantasyon üzerinden doğrulanmış sınırlamalar
**Yöntem:** `grep`, `read`, ve AST düzeyinde inceleme ile teyit edilmiştir.

---

## Özet: En Kritik 10 Handikap

| # | Handikap | Doğrulama | Önem |
|---|----------|-----------|------|
| 1 | **Tek-yazıcı modeli** — Uzun SELECT'ler yazma işlemlerini bloklar | `docs/ARCHITECTURE_FULL.md:521-524` | Yüksek |
| 2 | **MySQL prepared statement wire protocol desteklenmiyor** | `pkg/server/server.go:422` | Yüksek |
| 3 | **MySQL wire komut kümesi çok dar** (sadece 8 komut) | `pkg/protocol/mysql.go:508-535` | Yüksek |
| 4 | **Catalog tek `sync.RWMutex` — 16 dosyada `Lock()`** | `catalog_ddl.go:89,263,329,446,598,667,759,787,805,832,1043,1063` | Yüksek |
| 5 | **HNSW vektör indeksi RAM'e sığar, disk'e yazılmaz** | `pkg/catalog/vector.go:519` `json:"-"` | Orta-Çok Yüksek |
| 6 | **Composite foreign key enforcement yarım** | `pkg/catalog/z_foreign_key_test.go:498` `t.Skip(...)` | Orta |
| 7 | **Distributed/HA/clustering yok** — Sadece master-slave replikasyon | `pkg/replication/replication.go:20-29` | Yüksek |
| 8 | **WAL recovery entegrasyon testleri atlanıyor / belirsiz** | `pkg/engine/database_edge_test.go:513` `t.Skipf(...)` | Orta |
| 9 | **37+ MySQL protokol testi `t.Skip` ile atlanıyor** | `pkg/protocol/mysql_*_test.go` | Düşük-Orta |
| 10 | **WASM runtime'da `unimplemented opcode` fallback** | `pkg/wasm/runtime.go:731` | Düşük |

---

## 1. Mimari ve Eşzamanlılık Handikapları

### 1.1 Single-Writer Model
**Doğrulama:** `docs/ARCHITECTURE_FULL.md:521-524` açıkça belirtiyor.
```
### Single Writer Design
- Only one write transaction at a time
- Long-running SELECTs block writes (known limitation)
```
Bu, production OLTP yüklerinde ciddi bir darboğazdır. Aynı anda bir INSERT/UPDATE çalışırken diğer tüm yazıcılar kilitlenir.

### 1.2 Coarse-Grained Catalog Mutex
**Doğrulama:** `pkg/catalog/catalog_ddl.go`'da 12 ayrı yerde `c.mu.Lock()` çağrısı bulundu. `catalog_insert.go:81`, `catalog_update.go`, `catalog_delete.go`, `catalog_txn.go`, `catalog_fts.go`, `catalog_maintenance.go`, `catalog_json.go`, `catalog_cte.go`, `catalog_fdw.go`, `catalog_view.go`, `catalog_vector.go`, `catalog_returning.go` dosyalarında da `Lock()` var.

Bu, DDL (CREATE/DROP TABLE) sırasında tüm DML'in (INSERT/UPDATE/DELETE) durmasına yol açar. Page-level veya row-level locking mekanizması yoktur. `pkg/txn/manager.go` deadlock detection var ama bu yalnızca transaction seviyesindedir, catalog şema kilidi seviyesinde değil.

### 1.3 Online Schema Change Yok
Tüm DDL operasyonları `c.mu.Lock()` altında çalıştığı için, ALTER TABLE sırasında tüm okuma/yazma işlemleri bloklanır. PostgreSQL'in `ACCESS SHARE` vs `ACCESS EXCLUSIVE` gibi çoklu kilit seviyeleri yoktur.

---

## 2. MySQL Wire Protocol ve İstemci Uyumluluğu

### 2.1 Prepared Statement Execution Yok
**Doğrulama:** `pkg/server/server.go:422`
```go
return wire.NewErrorMessage(3, "prepared statement execution not yet supported via wire protocol")
```
Bu, MySQL client'ların (mysql CLI, DBeaver, Java JDBC) `PREPARE`/`EXECUTE` kullanamayacağı anlamına gelir. Sadece plain-text `QUERY` modu çalışır.

### 2.2 Dar Komut Kümesi
**Doğrulama:** `pkg/protocol/mysql.go:508-535`
Desteklenen komutlar: `QUIT`, `QUERY`, `PING`, `INIT_DB`, `STMT_PREPARE`, `STMT_EXECUTE`, `STMT_CLOSE`, `STMT_RESET`.

Eksik kritik komutlar: `FIELD_LIST`, `CREATE_DB`, `DROP_DB`, `REFRESH`, `SHUTDOWN`, `STATISTICS`, `PROCESS_INFO`, `CONNECT`, `PROCESS_KILL`, `DEBUG`, `CHANGE_USER`, `BINLOG_DUMP`, `TABLE_DUMP`, `REGISTER_SLAVE`, `RESET_CONNECTION`, `CLONE`, `GROUP_REPLICATION`.

Her desteklenmeyen komut `sendErrorPacket(0, "Unsupported command")` döner — MySQL hata kodu her zaman `0`, bu istemcileri şaşırtabilir.

### 2.3 User-Defined Variables (@var) Yok
**Doğrulama:** `pkg/protocol/mysql.go:544-547`
Sadece `@@version_comment` ve `@@max_allowed_packet` sabitleri whitelist'tedir. `SET @x = 1` gibi session variable'lar desteklenmiyor.

---

## 3. SQL ve Veri Tipi Sınırlamaları

### 3.1 Dokümantasyon ile Parser Arasındaki Uyumsuzluk
**Doğrulama:** `docs/SQL.md:24-29` sadece `INTEGER, TEXT, REAL, BOOLEAN, JSON` listeler. Ancak `pkg/query/parser.go:2324` ve `pkg/query/token.go:104` açıkça `TokenBlob`, `TokenDate`, `TokenTimestamp`, `TokenDatetime`, `TokenVector` parse ediyor.

Bu bir dokümantasyon handikabıdır — kullanıcı BLOB kullanabileceğini bilemez ama parser kabul eder.

### 3.2 Eksik SQL Özellikleri
- **`NATURAL JOIN`** — parser kabul etmiyor (`pkg/query/parser_extra_coverage_test.go:520-527`)
- **`USING` clause** — testte "may not be fully supported" notu (`pkg/query/parser_extra_coverage_test.go:541-548`)
- **`->>` (JSON double-arrow operator)** — `t.Log("->> operator not supported")` (`pkg/query/parser_extra_coverage_test.go:880-886`)
- **`REPLACE INTO`** standalone — yok (`pkg/query/parser_extra_coverage_test.go:963`)
- **`CREATE OR REPLACE`** — test notu yok ama AST'de muhtemelen yok
- **`IS DISTINCT FROM`** — eksik (`pkg/query/coverage_boost_test.go:569`)
- **`GROUPING SETS`, `ROLLUP`, `CUBE`** — parser/AST'de bulunamadı
- **`PIVOT/UNPIVOT`** — bulunamadı
- **`LATERAL JOIN`** — bulunamadı

### 3.3 Stored Procedure / Trigger Yarım
**Doğrulama:**
- `pkg/engine/engine_more_test.go:1718` `t.Skipf("CREATE PROCEDURE not supported: %v", err)`
- `pkg/engine/database_edge_test.go:277` aynı skip
- `pkg/catalog/catalog_ddl.go:1042-1049` `CreateProcedure` sadece AST'yi map'e kaydeder, **execute etmez**.
- `pkg/query/parser_extra_coverage_test.go:1532` `INSTEAD OF` trigger notu var.

### 3.4 Composite Foreign Key Enforcement Eksik
**Doğrulama:** `pkg/catalog/z_foreign_key_test.go:497-498`
```go
func TestForeignKeyEnforcerCompositeKey(t *testing.T) {
    t.Skip("Composite key foreign key validation not fully implemented")
```
Tek-sütun FK çalışıyor ama çok-sütunlu FK (örn. `(tenant_id, user_id)`) enforce edilmiyor.

---

## 4. İndeks ve Depolama Handikapları

### 4.1 HNSW Vektör İndeksi Kalıcı Değil
**Doğrulama:** `pkg/catalog/vector.go:519`
```go
type VectorIndexDef struct {
    // ...
    HNSW *HNSWIndex `json:"-"` // Runtime index, not persisted
}
```
`json:"-"` tag'i bu alanın serialize edilmediğini, yani veritabanı restart sonrası HNSW indeksinin yeniden `Build()` edilmesi gerektiğini gösterir. Büyük embedding dataset'lerinde bu dakikalar sürebilir.

### 4.2 FDW (Foreign Data Wrapper) Tam Materyalizasyon
**Doğrulama:** `CLAUDE.md:287`
```
Foreign tables are materialized into temporary B-trees at scan time
```
`pkg/fdw/fdw.go:18` `Scan()` tüm satırları `[][]interface{}` olarak döner. Bu, büyük CSV/PostgreSQL tablolarında memory/disk patlamasına yol açabilir. Ayrıca FDW read-only'dir.

---

## 5. Test ve Kalite Endişeleri

### 5.1 37+ Atlanan Test
**Doğrulama:** `pkg/protocol/` altında 37 `t.Skip("Cannot open database")` çağrısı:
- `mysql_integration_test.go`: 14
- `mysql_extended_test.go`: 6
- `mysql_deep_coverage_test.go`: 10
- `mysql_more_test.go`: 7

Bu testler "Cannot open database" ile atlanıyor — test altyapısındaki bir sorun, yoksa engine'in bir bug'ı mı net değil. İkisi de ciddi bir sinyaldir.

### 5.2 WAL Recovery Testi Atlanıyor
**Doğrulama:** `pkg/engine/database_edge_test.go:506-514`
```go
// Reopen - should recover from WAL
db2, err := Open(dbPath, &Options{...})
if err != nil {
    t.Skipf("Reopen with WAL recovery not supported: %v", err)
    return
}
```
WAL'in crash sonrası gerçekten çalıştığı kanıtlanmamıştır.

### 5.3 Stress Test Atlanıyor
**Doğrulama:** `pkg/engine/stress_test.go:182,386`
```go
t.Skip("skipping extended stress test")
t.Skip("backup manager not initialized")
```

### 5.4 golangci-lint Çok Minimal
**Doğrulama:** `.golangci.yml`
- Sadece 6 linter açık (`errcheck, gosimple, govet, ineffassign, staticcheck, unused`)
- `tests: false` — test kodları hiç lint'lenmiyor
- `gosec`, `revive`, `gocritic`, `gocyclo`, `dupl`, `prealloc` kapalı
- `cmd/cobaltdb-bench`, `cmd/debug`, `cmd/demo`, `cmd/realworld-test` lint dışı

### 5.5 Dokümantasyon Çiftleşmesi
`CLAUDE.md:214` ve `AGENTS.md:134` aynı cümleyi içeriyor: "Known Limitations: (No engine-level limitations currently tracked.)". Bu, gerçekte birçok sınırlama varken şeffaflık eksikliğidir.

---

## 6. Operasyonel ve Üretim Handikapları

### 6.1 Distributed / HA / Clustering Yok
**Doğrulama:** `pkg/` altında `cluster/`, `raft/`, `paxos/`, `2pc/` paketi yok.
`pkg/replication/replication.go:20-29` sadece `ModeAsync`, `ModeSync`, `ModeFullSync` master-slave replikasyon sunar. Otomatik failover, leader election, split-brain çözümü yoktur.

### 6.2 Audit Log Integrity Yok
`pkg/audit/` paketi var ama log kayıtlarının HMAC/imza ile bütünlük doğrulaması yapılmıyor. Yasal uyumluluk (compliance) için bu kritiktir.

### 6.3 Encryption Key Rotation Kanıtı Yok
`pkg/storage/encryption.go` dosyası incelendiğinde key rotation mekanizması görünmemektedir. AES-256-GCM aynı key ile çalışır.

### 6.4 Connection Pool Per-User Limit Yok
**Doğrulama:** `docs/PERFORMANCE.md:232` "Add per-user connection limits" gelecekte yapılacaklar listesindedir.

---

## 7. WASM Runtime Sınırlamaları

### 7.1 Streaming Sadece SELECT'te
**Doğrulama:** `pkg/wasm/runtime.go:136`
```go
return nil, fmt.Errorf("streaming not supported for non-SELECT queries")
```

### 7.2 Unimplemented Opcode Fallback
**Doğrulama:** `pkg/wasm/runtime.go:730-731`
```go
default:
    return fmt.Errorf("unimplemented opcode: 0x%02x", opcode)
```
WASM derleyici tam opcode setini desteklemiyor.

---

## Özet Karar

| Kategori | Ciddi Sorun | Orta | Hafif |
|----------|-------------|------|-------|
| Mimari | Single-writer, Catalog mutex, No HA | Composite FK, FDW materialization | — |
| MySQL Uyum | Prepared stmt yok, Dar komut kümesi | @var yok, Error code 0 | — |
| SQL | Stored proc execute yok, Natural Join yok | USING, ->> yok | — |
| Depolama | HNSW kalıcı değil | WAL recovery belirsiz | — |
| Test/Kalite | 37+ skip, lint minimal | Stress test skip | — |
| Güvenlik | Audit integrity yok | Key rotation yok | — |

**Tek cümle:** CobaltDB feature-rich bir embedded SQL engine'dir ama production OLTP olarak iddia ediyorsa **tek-yazıcı modeli**, **MySQL protokol uyumsuzlukları**, ve **coarse-grained locking** ciddi engellerdir. Dokümantasyonun "no limitations" demesi en büyük handikaptır çünkü kullanıcıları yanıltır.
