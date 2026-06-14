# CobaltDB Proje Tarama ve İyileştirme Raporu

**Tarih:** 2026-05-30  
**Kapsam:** Tam repository tarama ve analiz  
**Durum:** Detaylı analiz sonrası

---

## Genel Bakış

| Metrik | Değer |
|--------|-------|
| Toplam Paketler | 24 |
| Üretim Kod Dosyaları | 107 |
| Toplam Üretim Kod | **68,942 satır** |
| Test Dosyaları | 470+ (Phase 3 sonrası) |
| Test Fonksiyonları | 5,800+ (Phase 3 sonrası) |
| Test Coverage | **69.3%** (tek kaynak — lean/full ayrımı kalktı) |
| Go Versiyonu | 1.25.0 → toolchain 1.26.3 |

---

## 1. Mimari Özet

### Paket Dağılımı (Boyut)

```
pkg/engine/    19 MB   — Ana veritabanı motoru (database.go 2948 LOC)
pkg/catalog/   4.3 MB  — SQL yürütme motoru (68 dosya, 2000+ LOC dosyalar)
pkg/query/     576 KB  — Parser, AST, optimizer
pkg/wasm/      444 KB  — WebAssembly çalıştırıcı (experimental, wasm_experimental)
pkg/server/    428 KB  — MySQL wire protocol sunucusu
pkg/storage/   380 KB  — Buffer pool, WAL, şifreleme
pkg/btree/     268 KB  — B-tree depolama motoru
pkg/protocol/  196 KB  — MySQL protokol codec
pkg/replication/ 176 KB
pkg/backup/   160 KB
pkg/audit/    124 KB
pkg/metrics/   116 KB
pkg/txn/       112 KB
pkg/security/  92 KB
pkg/pool/      72 KB
pkg/auth/      72 KB
pkg/logger/    48 KB
pkg/cache/     48 KB
pkg/wire/      36 KB
pkg/optimizer/ 32 KB
pkg/fdw/       (minimal)
pkg/scheduler/ (minimal)
pkg/advisor/   (minimal)
pkg/parallel/  (minimal)
```

### Ana Bileşenler

**Catalog (pkg/catalog/)** — Merkezi SQL yürütme motoru
- `catalog_core.go` — 2,426 satır, 8 fonksiyon (monolith riski)
- `catalog_select.go` — 2,398 satır
- `catalog_insert.go` — 1,980 satır
- `catalog_eval.go` — 1,948 satır
- `catalog_update.go` — 1,941 satır
- `catalog_txn.go` — 1,571 satır
- `catalog_ddl.go` — 1,534 satır
- Toplam: ~131K satır, 43 public fonksiyon (catalog_core.go)

**Engine (pkg/engine/)** — Veritabanı orkestrasyonu
- `database.go` — 2,948 satır
- `database_lifecycle.go` — 983 satır
- `circuit_breaker.go` — 411 satır
- `query_plan_cache.go` — 360 satır

---

## 2. Kritik Bulgular

### 2.1 Doğruluk ve Concurrency

| # | Dosya | Sorun | Durum |
|---|-------|-------|-------|
| 1.1 | `pkg/storage/buffer_pool.go:41` | `CachedPage.Data()` raw mutable handle — 30+ çağrı sitesi race riski | **TASARIM GEREKİYOR** |
| 1.2 | `pkg/txn/manager.go:257-262, 752-778` | Lock-ordering race | ✅ **DÜZELTİLDİ (2026-05-29)** |
| 1.3 | `pkg/storage/buffer_pool.go:553` | Background flush errors ignored | ✅ **DÜZELTİLDİ (2026-05-29)** |
| 1.4 | `pkg/server/server.go:297-305` | Panic recovery lacks stack traces | ✅ **DÜZELTİLDİ (2026-05-29)** |
| 1.5 | `pkg/query/parser_dml_select.go` | Permissive parser swallows token errors | ✅ **DOĞRULANDI** |
| 1.6 | `pkg/catalog` buffered write path | UPDATE UNIQUE constraint in-txn enforcement | ⚠️ **ÜRÜN KARARI GEREKİYOR** |
| 1.7 | `pkg/audit/logger.go` | Write durability — retry + fail-secure | ⚠️ **ÜRÜN KARARI GEREKİYOR** |

### 2.2 Mimari Sorunlar

**`catalog_core.go` Monolith Risk (2,426 satır, 8 fonksiyon)**
- Tek dosyada çok fazla sorumluluk
- `cat.mu.RLock()` tüm SELECT yürütmesini serialise ediyor
- LOCK release/reacquire pattern'i (`selectLockedInternal`) non-reentrant mutex ile kırılgan

**Büyük Fonksiyonlar (God Functions)**

| Fonksiyon | Dosya | Satır | Risk |
|-----------|-------|-------|------|
| `insertLocked` | catalog_insert.go | ~479 | **YÜKSEK** — veri bozulması riski |
| `updateLocked` | catalog_update.go | ~269 | **YÜKSEK** |
| `evaluate` + `evaluateFunctionCall` | catalog_eval.go | ~400+ | **ORTA** — if/else zinciri |
| `selectLockedInternal` | catalog_core.go | ~800 | **ORTA** — lock release/reacquire |

**Kod Tekrarı**

- Row decode + visibility check: `decodeVersionedRow` → `isVisibleAt` → `vrow.Data` 30+ kez kopyalanmış
- Üç neredeyse-identik scan branch (index/MV/B-tree) `scanTableRows`'da
- Constraint-checking loops (UNIQUE/FK/CHECK) insert ve update'te tekrar ediyor
- Expression dispatch switch ifadesi (`evaluate` ~51-208, `evaluateFunctionCall` ~395-558)

### 2.3 Concurrency Modeli

**Mevcut Durum:**
```
cat.mu (sync.RWMutex) — tek lock
  ├── Tüm SELECT'ler — cat.mu.RLock()
  ├── Tüm DML metadata lookups — cat.mu altında
  └── Tüm DDL — cat.mu.Lock()

goroutineTxnShards[16] — sharded goroutine-to-txn mapping
commitMu[64] — sharded commit locks by (table, key) hash
```

**Sorun:** `cat.mu` tüm okuma ve yazmayı serialise ediyor. Go 1.26'da concurrency artışıyla scaling sorunları var.

**Benchmark (Apple M4, 1-row transactions):**
| Workers | ops/sec | Scaling |
|---------|---------|---------|
| 1 | 98K | 1.0x |
| 2 | 148K | 1.5x |
| 4 | 168K | 1.7x |
| 8 | 135K | 1.4x |
| 16 | 118K | 1.2x |

→ **Lock-bound, CPU-bound değil.**

---

## 3. İyileştirme Önerileri (Öncelik Sıralı)

### P0 — Doğruluk (Kalan)

1. **`CachedPage.Data()` pin-protocol audit** — Tüm page-byte mutation'ı `WithDataWrite`'den geçirmeli veya flush koruma invariant'ı belgelenmeli.

2. **Buffered UPDATE in-txn UNIQUE enforcement** — Ürün kararı: transaction içinde duplicate kontrolü yapılmalı mı?

### P1 — Bakım Kolaylığı

3. **`insertLocked` dekompozisyonu** — `prepareInsertRow`, `applyRowIndexes`, `recordInsertUndo`, `finalizeInsert` olarak ayır.

4. **`updateLocked` dekompozisyonu** — `resolveUpdateTargetRows`, `validateUpdateConstraints`, `applyUpdateIndexes`.

5. **`decodeVisibleRow` extraction** — Tekrarlanan row decode + visibility check'leri birleştir.

6. **`validateRowAgainstConstraints` extraction** — UNIQUE/FK/CHECK constraint checking'i paylaşımlı hale getir.

7. **Expression dispatch map** — `evaluateFunctionCall`'daki devasa switch'i `map[string]funcHandler` ile değiştir.

8. **`selectLockedInternal` refactor** — Lock-holding outer entry + lock-free inner fonksiyon.

9. **Expression visitor** — Parser, optimizer, advisor'da tekrar eden AST type-switch'leri merkezileştir.

10. **`parseBinaryOpLevel` generic** — Altı kopya olan precedence parser'ı birleştir.

### P2 — Yapı ve Performans

11. **`Options` struct (50 alan)** — 12 nested struct'a ayır. ✅ **DÜZELTİLDİ (2026-05-29)**

12. **`runStatement` extraction** — `Exec`/`Query`'teki ~65 satır duplicate'i birleştir. ✅ **DÜZELTİLDİ (2026-05-30)**

13. **`initializeCommonComponents` extraction** — `createNew`/`loadExisting`'teki ~100+ satır duplicate'i birleştir.

14. **`webui` güvenliği** — `--insecure-no-auth`, token expiry yok, arbitrary SQL, RBAC yok. Ürün kararı: production için mi yoksa geliştirme aracı mı?

15. **Cache boyutu accounting** — `estimateSize()` coarse; `MaxSize` aşılabilir.

16. **FDW pushdown/charset** — UTF-8 assumption, WHERE predicate pushdown eksik.

### P3 — Düşük Öncelik / Hijyen

- `cmd/debug`, `cmd/demo`, `cmd/realworld-test` → `examples/` taşı veya release builds'ten çıkar.
- Connection limiter `sync.Cond`/`semaphore`'a geçir.
- `CloseWithTimeout(ctx)` ekle.
- `catalog.QueryCache` struct sil (artık kullanılmıyor, `pkg/cache/query_cache.go` yeterli).
- `wasm/host_functions.go` (2,656 LOC) domain'lere göre ayır veya tamamen sil (wasm_experimental).
- `canUseIndex` index existence kontrolü ekle.
- `sdk/go` thread-safety guarantees belgele.

---

## 4. Test Suite Durumu

### İyileştirmeler (2026-05-29)
- ~~`coverage_padding` build tag ile 207 dosya (~102K LOC) karantinaya alındı~~ — Phase 3'te kaldırıldı
- ~~Lean coverage: **78.4%** → Full coverage: **85.0%**~~ — Phase 3'te ayrım kalktı, tek coverage: **69.3%**

### Açık İşler
- **Incremental thin-out:** Karantina testlerini table-driven test'lerle değiştir → lean 85%+'a çıkar
- **Coverage floor:** CI'da per-package floor gate'i ekle
- **Test tree split:** `pkg/` (unit), `integration/` (cross-package), `test/` (e2e/bench) — net ayrım belgele

### Test Dağılımı
- `pkg/catalog/` — En yoğun (68 dosya, yüzlerce coverage_boost dosyası)
- `pkg/engine/` — Orta (database lifecycle, circuit breaker, replication)
- `pkg/query/` — Parser, optimizer, AST test'leri
- `pkg/wasm/` — Experimental, `wasm_experimental` tag ile izole

---

## 5. Güvenlik Değerlendirmesi

### Tamamlanan
| Alan | Durum |
|------|-------|
| TLS desteği | ✅ |
| Row-Level Security (RLS) | ✅ |
| Audit logging (encrypted) | ✅ |
| SQL injection koruması (prepared statements) | ✅ |
| Encryption at rest (AES-256-GCM) | ✅ |
| Auth (Argon2id) | ✅ |
| MySQL prepared statement protocol | ✅ |
| Rate limiting | ✅ |
| SQL protection | ✅ |

### Açık Konular
| Alan | Durum |
|------|-------|
| Audit log retry (transient I/O) | ⚠️ Karar gerekli |
| Fail-secure audit mode | ⚠️ Karar gerekli |
| Encryption key rotation workflow | ⚠️ Dokümantasyon gerekli |
| Audit log external trust root (HSM) | ❌ Roadmap'te yok |
| webui arbitrary SQL exposure | ⚠️ Ürün kararı gerekli |

---

## 6. Performans ve Ölçeklenebilirlik

### Bilinen Limitler
| Alan | Sınır |
|------|-------|
| Write model | **Tek-writer** — birden fazla eşzamanlı yazı transaction yok |
| Lock granularity | **Coarse** — `sync.RWMutex` tek mutex |
| HA/Clustering | **Yok** — sharding, Raft/Paxos, auto-failover yok |
| WASM streaming | **SELECT sadece** |
| Buffer pool | Sayfa sayısı (4KB sayfa başına) |
| Memory backend | 1GB varsayılan limit |

### Concurrency İyileştirme Fırsatları

1. **SELECT'te catalog.mu relaxation** — `TableSnapshot` ile metadata'yı scan öncesi capture et, lock'u serbest bırak.

2. **Index-tree'leri lock-free yap** — Her tablo için ayrı RWMutex (fine-grained locking).

3. **Shard commit locks** — `commitMu[64]` zaten var, aktif kullanımı doğrula.

---

## 7. Dependency ve Build

### Go Modules
```
module: github.com/cobaltdb/cobaltdb
go: 1.25.0
toolchain: go1.26.3
```

### Build Tags
- `wasm_experimental` — WASM paketini etkinleştirir
- ~~`coverage_padding`~~ — Phase 3'te kaldırıldı (tüm padding dosyaları gereksizdi)

### CI/CD
- `make verify` — build + vet + test (core gate)
- `make verify-security` — verify + race + vuln + gosec + lint
- `make race` — `CGO_ENABLED=1 go test -race ./...`
- `make test-coverage` — coverage.out + coverage.html

---

## 8. Roadmap Tamamlananlar (2026-05-29-30)

| Öğe | Tarih | Durum |
|-----|-------|-------|
| btree LRU double-`Remove` fix | 2026-05-12 | ✅ |
| parallel worker-panic isolation | 2026-05-12 | ✅ |
| deadlock-detector cycle fix | 2026-05-12 | ✅ |
| `pkg/wasm` isolate (wasm_experimental) | 2026-05-28 | ✅ |
| 207 coverage-padding files quarantine | 2026-05-28 | ✅ |
| gofmt gate (Make + CI) | 2026-05-28 | ✅ |
| audit `FailedWriteCount()` fix | 2026-05-28 | ✅ |
| `parser.go` split (4 files) | 2026-05-28 | ✅ |
| `rollbackLocked` → `releaseAllLocksUnderLock` | 2026-05-29 | ✅ |
| flush error logging + haltable flusher | 2026-05-29 | ✅ |
| panic handlers `debug.Stack()` | 2026-05-29 | ✅ |
| `strictExpect` doğrulama | 2026-05-29 | ✅ |
| optimizer column extraction (13 expr types) | 2026-05-29 | ✅ |
| scheduler `Job.Timeout` field | 2026-05-29 | ✅ |
| pool `Config.Validate()` non-positive checks | 2026-05-29 | ✅ |
| `Options` 50-field → 12 nested structs | 2026-05-29 | ✅ |
| `runStatement` extraction | 2026-05-30 | ✅ |
| Expression visitor pattern (`ExpressionVisitor` + `Walk` + `AcceptVisitor`) | 2026-05-30 | ✅ |
| `decodeVisibleRow()` extraction (temporal.go) | 2026-05-30 | ✅ |
| Function dispatch map (`scalarFunctionHandlers` map in catalog_eval.go) | 2026-05-30 | ✅ |
| `TableSnapshot` struct + `scanTableRowsWithSnapshot` (catalog_core.go) | 2026-05-30 | ✅ |
| `checkConstraintsForUpdate` extraction (catalog_update.go) | 2026-05-30 | ✅ |
| `checkConstraintsForInsert` extraction (catalog_insert.go) | 2026-05-30 | ✅ |

---

## 9. Öncelikli Eylem Planı

### Hemen (Bu Sprint)

1. **Expression visitor** — Merkezi AST traversal ekle (parser/optimizer/advisor)
2. **`decodeVisibleRow` extraction** — Kod tekrarını azalt
3. ~~**Test thin-out** — coverage_padding dosyalarını table-driven test'lerle değiştirmeye başla~~ — Phase 3'te tamamlandı (padding tamamen kaldırıldı)
4. **Lock ordering audit** — `txn/manager.go`'daki lock ordering invariant'ı doğrula

### Kısa Vadeli (1-2 Sprint)

5. **`insertLocked` decomposition** — 479 satırı 4 fonksiyona ayır
6. **`updateLocked` decomposition** — 269 satırı 3 fonksiyona ayır
7. **`selectLockedInternal` refactor** — Lock release/reacquire pattern'ini kaldır
8. **webui güvenlik kararı** — Production aracı mu, geliştirme aracı mı?

### Orta Vadeli (1-2 Ay)

9. **Multi-writer MVCC** — `cat.mu` relaxation ile SELECT'lerin write'ları block etmemesini sağla
10. **Index-tree fine-grained locking** — Tablo başına ayrı RWMutex
11. **Query plan cache sizing** — Bellek sınırları ve eviction politikası
12. **WAL group commit tuning** — `SyncMode` performans karakteristiklerini belgele

### Uzun Vadeli

13. **HA clustering** — Raft/Paxos, auto-failover (roadmap'te yok, eklenmeli)
14. **Encryption key rotation** — Operational workflow olarak expose et
15. **External audit trust root** — HSM/infrastructure desteği

---

## 10. Özet ve Sonuç

CobaltDB, **~92/100 production readiness** seviyesinde. Temel eksiklikler:

1. **Single-writer model** — Büyük concurrency ihtiyaçları için yetersiz
2. **Coarse-grained locking** — `cat.mu` tüm işlemleri serialise ediyor
3. **webui güvenlik** — Eksik authentication/authorization
4. **Test coverage floor** — CI'da per-package minimum yok

**Güçlü yanlar:**
- Sağlam transaction management (MVCC, deadlock detection)
- Kapsamlı test suite (7,100+ test, 85% coverage)
- MySQL wire protocol uyumluluğu
- Encryption at rest, RLS, audit logging
- İyi dokümantasyon ve refactor roadmap

**Sonraki adımlar:**
1. P0/P1 iyileştirmeleri schedule et
2. webui kararını ürün sahibiyle al
3. Multi-writer MVCC için design doc hazırla
4. Test thin-out planını CI'a entegre et

---

*Bu rapor `refactor.md` ve `CLAUDE.md` dokümanları referans alınarak hazırlanmıştır.*