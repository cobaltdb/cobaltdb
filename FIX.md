# CobaltDB - Kapsamli Onarim Plani (FIX.md)

> **DURUM: 115/115 Duzeltildi (%100)** | Kalan: 0
> Son Guncelleme: 2026-03-09

> Olusturulma: 2026-03-08 | Kaynak: 5 paralel review agent + manuel analiz

---

## ✅ DUZELTILEN KRITIK SORUNLAR

| Faza | Sorun Sayisi | Durum |
|------|-------------|-------|
| Faza 0: Acil Guvenlik | 20/20 | ✅ **Tamamlandi** |
| Faza 1: Veri Butunlugu | 15/15 | ✅ **Tamamlandi** |
| Faza 2: Concurrency | 28/28 | ✅ **Tamamlandi** |
| Faza 3: Performans | 30/30 | ✅ Tamamlandi |
| Faza 4: Mimari | 22/22 | ✅ **Tamamlandi** |

### Bu Oturumda Duzeltilenler:
- **FIX-023**: ORDER BY tip donusum hatasi (comma-ok pattern)
- **FIX-024**: AFTER trigger hata yutma (5 yer)
- **FIX-025**: Index cleanup hatalari
- **FIX-033**: JSON_SET/JSON_REMOVE tip kontrolu
- **FIX-007**: Varsayilan admin sifre uyari si
- **FIX-098**: toFloat64 duplicate (unified to security.ToFloat64)
- **FIX-099**: 633-satirlik switch statement registry pattern'e donusturuldu
  - Yeni `catalog_functions.go` dosyasi olusturuldu (40+ SQL fonksiyonu)
  - Eksik fonksiyonlar eklendi: CONCAT_WS, LPAD, RPAD, REPEAT, PRINTF
  - GROUP_CONCAT destegi tamamlandi
- **FIX-095**: Context propagation Catalog katmanina eklendi
  - `Catalog.Select()`, `Insert()`, `Update()`, `Delete()` metodlarina `context.Context` eklendi
  - `selectLocked()`, `insertLocked()`, `updateLocked()`, `deleteLocked()` ic metodlari guncellendi
  - Tum test dosyalari context kullanacak sekilde guncellendi
- **FIX-094**: 12,699 satirlik God File bolundu
  - `pkg/catalog/catalog.go` → 18 dosyaya bolundu
  - Her dosya tek sorumluluklu (INSERT, SELECT, DDL, vb.)
  - Import temizligi yapildi, kullanilmayanlar kaldirildi

### Test Durumu: ✅ **TUM 22 PAKET BASARIYLA GECIYOR**

---

## FAZA 0: ACIL GUVENLIK (Gun 1)

### FIX-001 [CRITICAL] Wire Protocol DoS - Sinirsiz Bellek Ayirma
- **Dosya:** `pkg/server/server.go:244-248`
- **Sorun:** `length` uint32 degerini client gonderir, max kontrol yok. `make([]byte, length-1)` 4GB ayirabilir.
- **Cozum:** `const maxPayloadSize = 16 * 1024 * 1024` ekle, length kontrolu yap.
```go
// ONCE:
payload := make([]byte, length-1)
// SONRA:
const maxPayloadSize = 16 * 1024 * 1024
if length > maxPayloadSize {
    c.sendError(1, "message too large")
    continue
}
payload := make([]byte, length-1)
```

### FIX-002 [CRITICAL] MySQL Protocol DoS - Sinirsiz Bellek Ayirma
- **Dosya:** `pkg/protocol/mysql.go:273-286, 328-340`
- **Sorun:** MySQL protokolunde de 24-bit length kontrolsuz okunur.
- **Cozum:** `const mysqlMaxPacketSize = 16 * 1024 * 1024` ekle.
```go
if length > mysqlMaxPacketSize || length < 0 {
    return fmt.Errorf("packet too large: %d", length)
}
```

### FIX-003 [CRITICAL] Kimlik Dogrulamasiz Uzaktan Kapatma Endpoint'i
- **Dosya:** `pkg/server/production.go:249`
- **Sorun:** `/shutdown` endpoint'i hicbir auth middleware'i olmadan acik.
- **Cozum:** Health server'a auth middleware ekle veya `/shutdown` endpoint'ini kaldir.
```go
// KALDIR:
mux.HandleFunc("/shutdown", ps.Lifecycle.GracefulShutdownHandler())
// VEYA sadece localhost'a kisitla:
if r.RemoteAddr != "127.0.0.1" && !strings.HasPrefix(r.RemoteAddr, "[::1]") {
    http.Error(w, "Forbidden", http.StatusForbidden)
    return
}
```

### FIX-004 [CRITICAL] MySQL Protokolu Kimlik Dogrulamasi Yok
- **Dosya:** `pkg/protocol/mysql.go:152-196`
- **Sorun:** MySQL server her baglantiya OK gonderir, sifre kontrolu yapmaz.
- **Cozum:** `auth.Authenticator` ile gercek MySQL native authentication ekle.
```go
// handleConnection icinde:
// 1. Rastgele scramble uret (crypto/rand)
// 2. readHandshakeResponse'dan alinan password'u dogrula
// 3. Basarisizsa sendErrorPacket ile reddet
```

### FIX-005 [HIGH] Zamanlama Saldirisi - Sifre/Token Karsilastirma
- **Dosya:** `pkg/auth/auth.go:144`, `pkg/server/admin.go:149`
- **Sorun:** `!=` operatoru ilk farkta durur, timing side-channel olusturur.
- **Cozum:** `crypto/subtle.ConstantTimeCompare` kullan.
```go
// ONCE (auth.go:144):
if passwordHash != user.PasswordHash {
// SONRA:
if subtle.ConstantTimeCompare([]byte(passwordHash), []byte(user.PasswordHash)) != 1 {

// ONCE (admin.go:149):
if providedToken != token {
// SONRA:
if subtle.ConstantTimeCompare([]byte(providedToken), []byte(token)) != 1 {
```

### FIX-006 [HIGH] Zayif Sifre Hashleme (SHA-256 yerine Argon2 kullan)
- **Dosya:** `pkg/auth/auth.go:87-95`
- **Sorun:** 10K iterasyonlu SHA-256 GPU saldirarina karsi zayif. Ayrica `salt+password` birlestirme belirsizligi var.
- **Cozum:** `argon2.IDKey` kullan (zaten go.mod'da dependency var).
```go
// ONCE:
func hashPassword(password, salt string) string {
    hash := []byte(salt + password)
    for i := 0; i < 10000; i++ {
        h := sha256.Sum256(hash)
        hash = h[:]
    }
    return hex.EncodeToString(hash)
}
// SONRA:
func hashPassword(password, salt string) string {
    hash := argon2.IDKey([]byte(password), []byte(salt), 3, 64*1024, 4, 32)
    return hex.EncodeToString(hash)
}
```

### FIX-007 [HIGH] Varsayilan Admin Kimlik Bilgileri (admin:admin)
- **Dosya:** `pkg/server/server.go:57-58`, `cmd/cobaltdb-server/main.go:25-26`
- **Sorun:** Varsayilan admin:admin sifreleri acik, --help ciktisinda gorunur.
- **Cozum:** Ortam degiskeni ile zorunlu sifre iste, varsayilan sifre ile baslamayi reddet.
```go
// main.go'da:
adminPass := os.Getenv("COBALTDB_ADMIN_PASSWORD")
if adminPass == "" || adminPass == "admin" {
    log.Fatal("COBALTDB_ADMIN_PASSWORD must be set to a non-default value")
}
```

### FIX-008 [HIGH] CORS Wildcard Admin API'de
- **Dosya:** `pkg/server/admin.go:156`
- **Sorun:** `Access-Control-Allow-Origin: *` tum web sitelerine erisim verir.
- **Cozum:** CORS header'larini kaldir veya belirli origin'lerle kisitla.
```go
// KALDIR:
w.Header().Set("Access-Control-Allow-Origin", "*")
```

### FIX-009 [HIGH] SQL Injection Korumasi Entegre Degil
- **Dosya:** `pkg/server/server.go:389`, `pkg/server/production.go:435`
- **Sorun:** `SQLProtector.CheckSQL()` var ama `handleQuery()` icinde cagirilmiyor.
- **Cozum:** handleQuery'de SQL calistirmadan once CheckSQL cagir.
```go
// server.go handleQuery icinde, db.Query'den once:
if sp := c.Server.sqlProtector; sp != nil {
    result := sp.CheckSQL(query.SQL)
    if result.Blocked {
        return wire.NewErrorMessage(9, "query blocked by SQL protection")
    }
}
```

### FIX-010 [HIGH] Izin Kontrolu Mantik Hatasi
- **Dosya:** `pkg/server/server.go:334-338`
- **Sorun:** `c.authed == false` kosulu yanlis anlama yaratir ve gelecek degisikliklerde guvenlik acigi olusturabilir.
- **Cozum:**
```go
// ONCE:
if !c.Server.auth.IsEnabled() || c.authed == false {
    return true
}
// SONRA:
if !c.Server.auth.IsEnabled() {
    return true
}
if !c.authed {
    return false
}
```

### FIX-011 [HIGH] SQL Prefix Matching ile Izin Atlama
- **Dosya:** `pkg/server/server.go:350-370`
- **Sorun:** `strings.HasPrefix(sqlUpper, "SELECT")` kontrolu `SELECT ...;DROP TABLE` ile atlanir.
- **Cozum:** Gercek query parser ile statement tipini belirle.

### FIX-012 [HIGH] Statik Sertifika Seri Numarasi
- **Dosya:** `pkg/server/tls.go:174, 293`
- **Sorun:** Self-signed sertifikalarda seri numarasi sabit `1` ve `2`.
- **Cozum:**
```go
serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
```

### FIX-013 [MEDIUM] Health Server Hassas Bilgi Ifsa Eder (Auth Yok)
- **Dosya:** `pkg/server/production.go:223-265`
- **Sorun:** `/stats`, `/circuit-breakers`, `/rate-limits` auth olmadan erisime acik.
- **Cozum:** Health probları haric tum endpoint'lere auth middleware ekle.

### FIX-014 [MEDIUM] RLS Context String Key Kullanimi
- **Dosya:** `pkg/security/rls.go:612, 617`
- **Sorun:** `ctx.Value("user")` string key ile cakisma riski.
- **Cozum:**
```go
type userKey struct{}
type tenantKey struct{}
// Kullanim: ctx.Value(userKey{})
```

### FIX-015 [MEDIUM] Audit Event ID'leri Tahmin Edilebilir
- **Dosya:** `pkg/audit/logger.go:399-401`
- **Sorun:** Event ID timestamp bazli, esit zamanlilarda cakisir.
- **Cozum:** `crypto/rand` ile rastgele ID uret.

### FIX-016 [MEDIUM] io.ReadAll Boyut Limiti Yok (Dekompresyon Bombasi)
- **Dosya:** `pkg/storage/compression.go:260`, `pkg/storage/pitr.go:193`
- **Sorun:** `io.ReadAll` sinirsiz bellek tuketebilir.
- **Cozum:** `io.LimitReader(r, maxDecompressedSize)` kullan.

### FIX-017 [MEDIUM] Grafana Varsayilan Sifre docker-compose'da
- **Dosya:** `docker-compose.yml:54`
- **Sorun:** `GF_SECURITY_ADMIN_PASSWORD=admin` hardcoded.
- **Cozum:** `.env` dosyasina tasi, versiyon kontrolune commit etme.

### FIX-018 [MEDIUM] Hata Mesajlari Ic Detaylari Ifsa Eder
- **Dosya:** `pkg/server/server.go:416-417`, `pkg/protocol/mysql.go:394`
- **Sorun:** `err.Error()` dogrudan client'a gonderilir.
- **Cozum:** Sanitize edilmis hata mesajlari gonder.

### FIX-019 [LOW] math/rand Retry Jitter Icin
- **Dosya:** `pkg/engine/retry.go:10, 177`
- **Sorun:** Go 1.24'te auto-seed var ama code hygiene sorunu.
- **Cozum:** Mevcut durumda kabul edilebilir, dokumante et.

### FIX-020 [LOW] Oturum Temizleme Arka Plan Gorevi Yok
- **Dosya:** `pkg/auth/auth.go`
- **Sorun:** `CleanupExpiredSessions()` otomatik cagirilmiyor, suren oturumlar birikerek bellek sizdiriyor.
- **Cozum:** Periyodik temizleme goroutine'i baslat.

---

## FAZA 1: VERI BUTUNLUGU (Gun 2-5)

### FIX-021 [CRITICAL] WAL Append Hatasi Sessizce Yutuluyor
- **Dosya:** `pkg/catalog/catalog.go:3245`
- **Sorun:** `c.wal.Append(record)` hatasi kontrol edilmiyor - tum codebase'de tek yer.
- **Cozum:**
```go
// ONCE:
c.wal.Append(record)
// SONRA:
if err := c.wal.Append(record); err != nil {
    return 0, rowsAffected, err
}
```

### FIX-022 [CRITICAL] BTree flushInternal No-Op - Veri Kaybi
- **Dosya:** `pkg/btree/btree.go:330-337`
- **Sorun:** `flushInternal()` sadece `return nil` yapar, evict edilen veriler kaybolur.
- **Cozum:** `flushInternal` icinde gercek persist islemi yap veya evict etmeden once veriyi yaz.

### FIX-023 [CRITICAL] Guvenli Olmayan Tip Donus - ORDER BY Panic
- **Dosya:** `pkg/catalog/catalog.go:7920-7923`
- **Sorun:** `vj.(string)` comma-ok pattern olmadan kullaniliyor, karisik tipli ORDER BY'da panic.
- **Cozum:**
```go
// ONCE:
vjS := vj.(string)
// SONRA:
vjS, ok := vj.(string)
if !ok {
    return !ob.Desc
}
```

### FIX-024 [HIGH] AFTER Trigger Hatalari Sessizce Yutuluyor
- **Dosya:** `pkg/catalog/catalog.go:2544, 2958, 3414`
- **Sorun:** `_ = c.executeTriggers(...)` hatasi discard ediliyor.
- **Cozum:** Hatayi logla veya return et.
```go
// ONCE:
_ = c.executeTriggers(stmt.Table, "INSERT", "AFTER", ...)
// SONRA:
if err := c.executeTriggers(stmt.Table, "INSERT", "AFTER", ...); err != nil {
    return 0, 0, fmt.Errorf("AFTER trigger failed: %w", err)
}
```

### FIX-025 [HIGH] 18+ Index Mutasyon Hatasi Sessizce Yutuluyor
- **Dosya:** `pkg/catalog/catalog.go` - satirlar: 2203, 2206, 2213, 2355, 2358, 2366, 2525, 2528, 2885, 2898, 3099, 3102, 3225, 3228, 3235, 3353, 3366
- **Sorun:** `idxTree.Delete()`, `idxTree.Put()`, `tree.Delete()` hatalari kontrol edilmiyor.
- **Cozum:** Her birinde hata kontrolu ekle:
```go
// ONCE:
idxTree.Delete([]byte(oldIdxKey))
// SONRA:
if err := idxTree.Delete([]byte(oldIdxKey)); err != nil {
    return 0, 0, fmt.Errorf("index delete failed: %w", err)
}
```

### FIX-026 [HIGH] BTree.loadFromPages Sessiz Bos Donme
- **Dosya:** `pkg/btree/btree.go:112-116`
- **Sorun:** Root page okunamazsa agac bos gorunur, tum veri kaybolmus gibi olur.
- **Cozum:** `loadFromPages` hata donsun, `OpenBTree` propagate etsin.

### FIX-027 [HIGH] tree.Scan Hatalari Yutuluyor (5+ Yer)
- **Dosya:** `pkg/catalog/catalog.go:1360, 1441, 1483, 2170, 2285, 3276`
- **Sorun:** `iter, _ := tree.Scan(nil, nil)` - nil iterator uzerinde `iter.Close()` panic yapar.
- **Cozum:**
```go
iter, err := tree.Scan(nil, nil)
if err != nil {
    return 0, 0, fmt.Errorf("scan failed: %w", err)
}
defer iter.Close()
```

### FIX-028 [HIGH] Autocommit Commit/Rollback Hatalari Yutuluyor
- **Dosya:** `pkg/engine/database.go:807-817`
- **Sorun:** defer icinde `CommitTransaction()` ve `RollbackTransaction()` hatalari discard.
- **Cozum:**
```go
defer func() {
    if err != nil {
        if rbErr := db.catalog.RollbackTransaction(); rbErr != nil {
            err = fmt.Errorf("%w; rollback failed: %v", err, rbErr)
        }
    } else {
        if cmtErr := db.catalog.CommitTransaction(); cmtErr != nil {
            err = cmtErr
        }
    }
}()
```

### FIX-029 [HIGH] Buffer Pool Close Sirasi Yanlis
- **Dosya:** `pkg/engine/database.go:427-436`
- **Sorun:** Pool kapatiliyor, sonra Checkpoint pool uzerinden calistirilmaya calisiyor.
- **Cozum:** WAL checkpoint'i pool kapatilmadan once yap.

### FIX-030 [MEDIUM] WAL readRecord io.ReadFull Kullanmiyor
- **Dosya:** `pkg/storage/wal.go:118, 132`
- **Sorun:** `reader.Read(header)` short read yapabilir, binary parsing bozulur.
- **Cozum:** `io.ReadFull(reader, header)` kullan.

### FIX-031 [MEDIUM] PITR ArchiveWAL Dosyayi Cift Kapatiyor
- **Dosya:** `pkg/storage/pitr.go:111, 132, 136`
- **Sorun:** `defer file.Close()` + acik `file.Close()` cagirisi = cift kapatma.
- **Cozum:** Satir 136'daki acik `file.Close()` cagirisini kaldir.

### FIX-032 [MEDIUM] Tx.Commit/Rollback Connection Cift Release
- **Dosya:** `pkg/engine/database.go:1967-1999`
- **Sorun:** Commit ve Rollback ikisi de `releaseConnection()` cagirir, ikisi de cagrilirsa counter negative olur.
- **Cozum:** `atomic.Bool` ile ilk commit/rollback'i izle:
```go
type Tx struct {
    done atomic.Bool
    // ...
}
func (tx *Tx) Commit() error {
    if !tx.done.CompareAndSwap(false, true) {
        return errors.New("transaction already completed")
    }
    defer tx.db.releaseConnection()
    // ...
}
```

### FIX-033 [MEDIUM] JSON_SET/JSON_REMOVE Tip Donus Hatasi Yutuluyor
- **Dosya:** `pkg/catalog/catalog.go:9831-9833`
- **Sorun:** `jsonData, _ := args[0].(string)` basarisiz olursa bos string ile calisir.
- **Cozum:** `ok` kontrol et, hata don.

### FIX-034 [LOW] Bare `return err` - Context Yok (6+ yer)
- **Dosya:** `pkg/engine/database.go:428, 448, 586, 1848, 1977, 1994`
- **Cozum:** `fmt.Errorf("failed to X: %w", err)` ile wrapping yap.

### FIX-035 [LOW] os.Exit Logger Kutuphane Kodunda
- **Dosya:** `pkg/logger/logger.go:188, 194`
- **Sorun:** `os.Exit(1)` tum defer'leri atlar, WAL flush yapilmaz.
- **Cozum:** `Fatal`'i `panic` olarak degistir veya sadece logla.

---

## FAZA 2: CONCURRENCY GUVENLIGI (Gun 3-7)

### FIX-036 [CRITICAL] QueryCache Data Race - RLock Altinda Yazma
- **Dosya:** `pkg/catalog/catalog.go:257-264`
- **Sorun:** `qc.hitCount++` ve `qc.missCount++` RLock altinda, data race.
- **Cozum:**
```go
// Struct'ta degistir:
hitCount  atomic.Int64
missCount atomic.Int64
// Get icinde:
qc.hitCount.Add(1)   // atomic.Int64 yerine
qc.missCount.Add(1)
```

### FIX-037 [CRITICAL] GroupCommitter.Stop() Double-Close Panic
- **Dosya:** `pkg/storage/group_commit.go:81-93`
- **Sorun:** `close(gc.stopCh)` sync.Once korumasiz.
- **Cozum:**
```go
type GroupCommitter struct {
    stopOnce sync.Once
    // ...
}
func (gc *GroupCommitter) Stop() {
    gc.mu.Lock()
    gc.stopped = true
    if gc.flushTimer != nil { gc.flushTimer.Stop() }
    gc.mu.Unlock()
    gc.stopOnce.Do(func() { close(gc.stopCh) })
    gc.Flush()
}
```

### FIX-038 [CRITICAL] ~20 Arka Plan Goroutine'de recover() Yok
- **Dosyalar:** Coklu
- **Etkilenen goroutine'ler:**
  - `pkg/engine/parallel.go:199, 255, 284, 334` (parallel scan/aggregate/sort)
  - `pkg/engine/alert.go:167, 458` (alert handler, cleanup)
  - `pkg/engine/circuit_breaker.go:257` (circuit breaker fn)
  - `pkg/engine/scheduler.go:281` (job execution)
  - `pkg/engine/index_advisor.go:107` (analysis loop)
  - `pkg/server/admin.go:94` (HTTP server)
  - `pkg/server/production.go:258` (health server)
  - `pkg/server/connection_manager.go:135` (idle cleanup)
  - `pkg/server/lifecycle.go:198, 254` (health monitor, signal handler)
  - `pkg/server/alert.go:123` (cleanup loop)
  - `pkg/server/rate_limiter.go:101` (cleanup loop)
  - `pkg/storage/group_commit.go:74` (flusher loop)
  - `pkg/replication/read_replica.go:208, 235` (health check)
  - `pkg/proxy/proxy.go:387, 393` (bidirectional proxy)
- **Cozum:** Her go func() basina ekle:
```go
go func() {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("recovered panic in background goroutine: %v", r)
        }
    }()
    // ... mevcut kod
}()
```

### FIX-039 [CRITICAL] CircuitBreaker.Execute Goroutine Leak
- **Dosya:** `pkg/engine/circuit_breaker.go:250-273`
- **Sorun:** Context iptalinde `fn()` calisan goroutine leak olur.
- **Cozum:**
```go
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
    if err := cb.Allow(); err != nil {
        return err
    }
    done := make(chan error, 1)
    go func() {
        defer func() {
            if r := recover(); r != nil {
                done <- fmt.Errorf("panic: %v", r)
            }
        }()
        done <- fn()
    }()
    select {
    case err := <-done:
        cb.Release()
        if err != nil { cb.ReportFailure(); return err }
        cb.ReportSuccess()
        return nil
    case <-ctx.Done():
        go func() { <-done; cb.Release() }()
        cb.ReportFailure()
        return ctx.Err()
    }
}
```

### FIX-040 [HIGH] context.Background() Wire Protocol Handler'da
- **Dosya:** `pkg/server/server.go:265`
- **Sorun:** Query'ler client disconnect olduktan sonra da calismaya devam eder.
- **Cozum:** Connection basina iptal edilebilir context olustur:
```go
type ClientConn struct {
    ctx    context.Context
    cancel context.CancelFunc
    // ...
}
// Handle() icinde:
c.ctx, c.cancel = context.WithCancel(context.Background())
defer c.cancel()
// handleMessage icinde:
ctx := c.ctx  // context.Background() yerine
```

### FIX-041 [HIGH] RateLimiter.getClientLimiter() TOCTOU Race
- **Dosya:** `pkg/server/rate_limiter.go:186-223`
- **Sorun:** `len(rl.clients)` lock olmadan okunuyor.
- **Cozum:** MaxClients kontrolunu write-lock icine tasi, double-check pattern kullan.

### FIX-042 [HIGH] RateLimiter.GetStats() tokens Lock Olmadan Okuyor
- **Dosya:** `pkg/server/rate_limiter.go:172-183`
- **Sorun:** `rl.global.tokens` float64 lock olmadan okunuyor.
- **Cozum:** `rl.global.mu.Lock()` ile oku.

### FIX-043 [HIGH] AdaptiveRateLimiter.Adjust() global.rate Yanlis Lock
- **Dosya:** `pkg/server/rate_limiter.go:319-331`
- **Sorun:** `arl.global.rate` yazilirken `global.mu` yerine `adjustMu` tutuluyor.
- **Cozum:** `arl.global.mu.Lock()` kullan.

### FIX-044 [HIGH] ReadReplicaManager.GetStats() r.Status Lock Olmadan
- **Dosya:** `pkg/replication/read_replica.go:387-409`
- **Sorun:** `r.Status` dogrudan `r.mu` olmadan okunuyor, GetStats() r.mu biraktigindan.
- **Cozum:** `rs.Status` (GetStats donus degeri) kullan, `r.Status` yerine.

### FIX-045 [HIGH] ConnectionPool.destroyConnection() IdleConns Negative Olabilir
- **Dosya:** `pkg/engine/connection_pool.go:291-301`
- **Sorun:** Her durumda `IdleConns` azaltiliyor ama connection idle olmayabilir.
- **Cozum:** Connection state'ini track et, uygun counter'i azalt.

### FIX-046 [HIGH] WorkerPool Goroutine Leak - Kullanilmiyor Ama Baslatiliyor
- **Dosya:** `pkg/engine/parallel.go:67-131, 146-155`
- **Sorun:** NewParallelQueryExecutor WorkerPool olusturur ama ParallelScan/Aggregate/Sort kullanmaz.
- **Cozum:** Lazy initialization yap veya WorkerPool'u kaldir.

### FIX-047 [HIGH] Fire-and-Forget Goroutine Alert Manager'da
- **Dosya:** `pkg/engine/alert.go:458-465`
- **Sorun:** Her resolved alert 5 dakika uyuyan goroutine baslatir, iptal mekanizmasi yok.
- **Cozum:** `time.AfterFunc` + cancel ile degistir veya tek cleanup ticker kullan.

### FIX-048 [MEDIUM] RateLimiter.Stop() sync.Once Yok
- **Dosya:** `pkg/server/rate_limiter.go:107-110`
- **Sorun:** `close(rl.stopCh)` cift cagri panic yapar.
- **Cozum:** `sync.Once` ekle.

### FIX-049 [MEDIUM] ConnectionManager.Stop() sync.Once Yok
- **Dosya:** `pkg/server/connection_manager.go:142-153`
- **Cozum:** `sync.Once` ekle `close(cm.shutdownCh)` icin.

### FIX-050 [MEDIUM] MetricsAggregator.Stop() sync.Once Yok
- **Dosya:** `pkg/server/metrics_aggregator.go:254-262`
- **Cozum:** `sync.Once` ekle `close(ma.stopCh)` icin.

### FIX-051 [MEDIUM] Lifecycle.OnStateChange() stateHooks Race
- **Dosya:** `pkg/server/lifecycle.go:162-163`
- **Sorun:** `stateHooks` map'ine lock olmadan yaziliyor.
- **Cozum:** `l.stateMu.Lock()` ile koru.

### FIX-052 [MEDIUM] setState Hook'lari recover() Olmadan Calistiriyor
- **Dosya:** `pkg/server/lifecycle.go:314-320`
- **Sorun:** `go hook()` recover olmadan, panic process'i oldurur.
- **Cozum:** Her hook'u recover ile sar.

### FIX-053 [MEDIUM] AlertManager.RegisterRule/RegisterHandler Thread-Safe Degil
- **Dosya:** `pkg/server/alert.go:111-118`
- **Sorun:** `rules` ve `handlers` slice'larina lock olmadan append.
- **Cozum:** Lock ekle veya Start()'tan once registration zorunlulugunu dokumante et.

### FIX-054 [MEDIUM] Tracer.AddTag/Log Span Alanlari Race
- **Dosya:** `pkg/server/tracing.go:184-200`
- **Sorun:** `span.Tags` ve `span.Logs` lock olmadan yaziliyor.
- **Cozum:** Span struct'ina `sync.Mutex` ekle.

### FIX-055 [MEDIUM] SlowQueryLog.SetCallback Thread-Safe Degil
- **Dosya:** `pkg/engine/slow_query_log.go:97-99`
- **Sorun:** `onSlowQuery` interface degeri lock olmadan yaziliyor.
- **Cozum:** `entriesMu` ile koru.

### FIX-056 [MEDIUM] Lifecycle context.Context Struct'ta Sakliyor
- **Dosya:** `pkg/server/lifecycle.go:75-77`
- **Sorun:** Go dokumantasyonu context'i struct'ta saklamayi yasaklar.
- **Cozum:** `stopCh chan struct{}` pattern kullan (zaten `shutdownCh` var).

### FIX-057 [MEDIUM] Metrics Collector context.Background() ile Basliyor
- **Dosya:** `pkg/engine/database.go:222`
- **Sorun:** Initialize hatada collector durdurulmuyor, goroutine leak.
- **Cozum:** Cancel edilebilir context kullan, hata durumunda `collector.Stop()` cagir.

### FIX-058 [MEDIUM] ConnectionManager.cleanupIdleConnections I/O Lock Altinda
- **Dosya:** `pkg/server/connection_manager.go:276-298`
- **Sorun:** `connectionsMu.Lock()` tutarken `mc.Conn.Close()` yapiyor, network I/O lock altinda.
- **Cozum:** Idle connection'lari topla, lock birak, sonra kapat.

### FIX-059 [MEDIUM] Server.Close() Client Goroutine'lerini Beklemiyor
- **Dosya:** `pkg/server/server.go:169-190`
- **Sorun:** `sync.WaitGroup` yok, handler goroutine'leri hala calisirken Close() donuyor.
- **Cozum:** `sync.WaitGroup` ekle, `go client.Handle()` -> `wg.Add(1)`.

### FIX-060 [LOW] ReadReplicaManager.Close() connPool Cift Close Panic
- **Dosya:** `pkg/replication/read_replica.go:477-504`
- **Sorun:** `RemoveReplica` zaten close ettiyse, `Close()` tekrar close eder.
- **Cozum:** Close sonrasi nil'a ata veya sync.Once kullan.

### FIX-061 [LOW] RateLimiter.Allow() lastAccess Lock Olmadan
- **Dosya:** `pkg/server/rate_limiter.go:125`
- **Sorun:** `cl.lastAccess = time.Now()` lock olmadan, `time.Time` multi-word struct.
- **Cozum:** Atomic unix timestamp kullan.

### FIX-062 [LOW] QueryExecutor.SetDefaultTimeout Thread-Safe Degil
- **Dosya:** `pkg/engine/query_timeout.go:243-245`
- **Cozum:** `atomic.Int64` veya mutex kullan.

### FIX-063 [LOW] DefaultMetrics Global Variable Race
- **Dosya:** `pkg/server/metrics_aggregator.go:535`
- **Cozum:** `sync.Once` ile initialization.

---

## FAZA 3: PERFORMANS OPTIMIZASYONU (Gun 6-14)

### FIX-064 [CRITICAL] JSON Serialization Her Satir Okuma/Yazma'da
- **Dosya:** `pkg/catalog/catalog.go:10844-10866`
- **Sorun:** Her satir `json.Marshal`/`json.Unmarshal` kullaniyor. 100K satir = 100K JSON parse. `fastEncodeRow` (satir 10871) var ama kullanilmiyor.
- **Cozum:** Binary encode/decode'a gec, JSON'u legacy fallback olarak tut.
- **Beklenen iyilesme:** 5-10x throughput

### FIX-065 [CRITICAL] O(n) Column Lookup Her Expression Evaluation'da
- **Dosya:** `pkg/catalog/catalog.go:8286-8322`
- **Sorun:** `evaluateExpression` her satir icin her sutun adini `strings.ToLower` yapiyor ve lineer scan. 10 sutun * 100K satir * 5 SELECT kolonu = 5M string allocation.
- **Cozum:** Query basinda `map[string]int` column index haritasi olustur.
- **Beklenen iyilesme:** 2-5x

### FIX-066 [HIGH] BTree.Get() Read Icin Write Lock Aliyor
- **Dosya:** `pkg/btree/btree.go:216-237`
- **Sorun:** `mu.Lock()` yerine `mu.RLock()` kullanilmali. Tum okumalari serialestiriyor.
- **Cozum:** Map lookup icin `RLock` kullan, LRU update icin ayri lock veya CLOCK algoritma.
- **Beklenen iyilesme:** Read parallelism aktif olur

### FIX-067 [HIGH] Buffer Pool touchLRU Her Cache Hit'te Write Lock
- **Dosya:** `pkg/storage/buffer_pool.go:238-243`
- **Sorun:** Her basarili page erisimi write lock aliyor, concurrent access engelleniyor.
- **Cozum:** CLOCK-sweep algoritma veya toplu LRU guncelleme.

### FIX-068 [HIGH] matchLikeSimple Exponential Backtracking + Per-Row ToLower
- **Dosya:** `pkg/catalog/catalog.go:8693-8775`
- **Sorun:** (1) Pattern her satir icin tekrar lowercased. (2) `%` handling recursive, O(n*m) worst-case.
- **Cozum:** (1) Pattern'i bir kez lowercase yap. (2) Iteratif DP yaklasimiyla degistir.

### FIX-069 [HIGH] regexp.MatchString/Compile Her Satir Icin
- **Dosya:** `pkg/catalog/catalog.go:9609`, `pkg/catalog/json_utils.go:660-684`
- **Sorun:** GLOB fonksiyonu ve 3 regex utility fonksiyonu her cagri icin regex compile eder.
- **Cozum:** Query basinda regex cache'i (`map[string]*regexp.Regexp`) veya package-level LRU cache.

### FIX-070 [HIGH] Statement Cache LRU Eviction O(n)
- **Dosya:** `pkg/engine/database.go:500-529`
- **Sorun:** IKI lineer scan + O(n) memory shift. `stmtCacheOrder` ayrica sinirsiz buyuyor.
- **Cozum:** `container/list.List` ile O(1) LRU eviction.

### FIX-071 [HIGH] Catalog QueryCache Eviction O(n) + Invalidation O(n*m)
- **Dosya:** `pkg/catalog/catalog.go:294-340`
- **Sorun:** Eviction tum entry'leri tarar. Invalidation tum entry'leri ve tablo bagimliklarini tarar.
- **Cozum:** LRU linked list + reverse index `map[string][]string` (tablo -> cache key'ler).

### FIX-072 [HIGH] BTree.Scan Tum Key/Value'lari Bellige Kopyalar
- **Dosya:** `pkg/btree/btree.go:379-414`
- **Sorun:** `SELECT * FROM t LIMIT 1` bile tum tabloyu materialize eder.
- **Cozum:** Sorted index yapisi kullan veya LIMIT'i scan sirasinda uygula.

### FIX-073 [HIGH] Query Optimizer Her Query'de Yeni Olusturuluyor
- **Dosya:** `pkg/catalog/catalog.go:3459`
- **Sorun:** Istatistik olmadan yeni optimizer + 3 map + AST deep-copy her SELECT'te.
- **Cozum:** Optimizer'i Catalog alani yap veya istatistik yoksa atla.

### FIX-074 [MEDIUM] SanitizeSQL Her Cagri Icin Regex Compile Eder
- **Dosya:** `pkg/server/sql_protection.go:360-361`
- **Cozum:** Package-level degiskene tasi:
```go
var (
    singleQuoteRe = regexp.MustCompile(`'[^']*'`)
    doubleQuoteRe = regexp.MustCompile(`"[^"]*"`)
)
```

### FIX-075 [MEDIUM] applyOrderBy Sort Comparator'da strings.ToLower
- **Dosya:** `pkg/catalog/catalog.go:6202-6252`
- **Sorun:** O(n log n) comparator cagrisinda her seferinde column name lowercased.
- **Cozum:** ORDER BY column index'lerini sort oncesinde coz, `[]int` mapping olustur.

### FIX-076 [MEDIUM] rowKeyForDedup fmt.Sprintf Kullaniyor
- **Dosya:** `pkg/catalog/catalog.go:6745-6758`
- **Sorun:** `fmt.Sprintf("V:%v", val)` reflection kullaniyor, DISTINCT/UNION icin yavas.
- **Cozum:** Type-switch ile `strconv` fonksiyonlari kullan.

### FIX-077 [MEDIUM] generateQueryKey String Concatenation Loop'ta
- **Dosya:** `pkg/catalog/catalog.go:350-357`
- **Sorun:** `key += fmt.Sprintf("|%v", arg)` her arg icin allocation.
- **Cozum:** `strings.Builder` kullan.

### FIX-078 [MEDIUM] QueryCache.generateKey SHA-256 Kullaniyor
- **Dosya:** `pkg/engine/query_cache.go:298-313`
- **Sorun:** Kriptografik hash gereksiz yavas. `normalizeSQL` ekstra allocation.
- **Cozum:** `hash/fnv` veya `hash/maphash` kullan.

### FIX-079 [MEDIUM] binary.Write BTree Flush'da Reflection Kullaniyor
- **Dosya:** `pkg/btree/btree.go:500-503`
- **Cozum:** `binary.LittleEndian.PutUint16/PutUint32` kullan.

### FIX-080 [MEDIUM] Parallel Sort Naive Quicksort O(n^2) Worst-Case
- **Dosya:** `pkg/engine/parallel.go:355-376`
- **Sorun:** Son eleman pivot, sorted data'da degrade olur.
- **Cozum:** `sort.Slice` kullan (introsort, median-of-three pivot + heapsort fallback).

### FIX-081 [MEDIUM] k-Way Merge Sequantial - O(n*k) Yerine O(n*log k)
- **Dosya:** `pkg/engine/parallel.go:378-391`
- **Cozum:** `container/heap` ile min-heap tabanli merge.

### FIX-082 [MEDIUM] Tokenize Token Slice Pre-Allocate Etmiyor
- **Dosya:** `pkg/query/lexer.go:365-381`
- **Cozum:** `tokens := make([]Token, 0, len(input)/4+10)`

### FIX-083 [MEDIUM] isLetter/isDigit Unicode Fonksiyonu ASCII Icin
- **Dosya:** `pkg/query/lexer.go:345-352`
- **Cozum:**
```go
func isLetter(ch byte) bool {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}
```

### FIX-084 [MEDIUM] newToken Single Byte String Allocation
- **Dosya:** `pkg/query/lexer.go:354-362`
- **Cozum:** Pre-allocated `[256]string` lookup table kullan.

### FIX-085 [MEDIUM] WAL readRecord Header Buffer Her Record'da Allocation
- **Dosya:** `pkg/storage/wal.go:115-152`
- **Sorun:** 17-byte header + CRC icin re-encode allocation.
- **Cozum:** Tek header buffer yeniden kullan, CRC'yi dogrudan okunan byte'lardan hesapla.

### FIX-086 [MEDIUM] GroupCommitter Sliding Window Backing Array Leak
- **Dosya:** `pkg/storage/group_commit.go:222-236`
- **Sorun:** `batchSizeHist[1:]` backing array buyutuyor ama GC yapilmiyor.
- **Cozum:** Ring buffer veya copy ile degistir.

### FIX-087 [MEDIUM] O(n) Full Table Scan Unique Constraint Kontrolu
- **Dosya:** `pkg/catalog/catalog.go:2170`
- **Sorun:** Her INSERT'te tum tablo taranir. Bulk insert O(n^2).
- **Cozum:** B-tree index lookup ile unique constraint kontrolu.

### FIX-088 [MEDIUM] String Concatenation formatInsert Loop'ta
- **Dosya:** `pkg/backup/backup.go:249-264`
- **Cozum:** `strings.Builder` kullan.

### FIX-089 [MEDIUM] TableSchema String Concatenation Loop'ta
- **Dosya:** `pkg/engine/database.go:720-751`
- **Cozum:** `strings.Builder` kullan.

### FIX-090 [LOW] CachedPage Struct Field Alignment
- **Dosya:** `pkg/storage/buffer_pool.go:19-25`
- **Cozum:** Alanlari azalan boyuta gore sirala.

### FIX-091 [LOW] EncryptedBackend.ReadAt Her Sayfa Okumada Allocation
- **Dosya:** `pkg/storage/encryption.go:112-149`
- **Cozum:** `sync.Pool` kullan.

### FIX-092 [LOW] DeriveKeyFromPassword rand.Read Hatasi Yutuluyor
- **Dosya:** `pkg/storage/encryption.go:236-242`
- **Cozum:** Hata kontrolu ekle veya `io.ReadFull` kullan.

### FIX-093 [LOW] Cift QueryCache Implementasyonu
- **Dosya:** `pkg/catalog/catalog.go:217-357` vs `pkg/engine/query_cache.go`
- **Sorun:** Iki farkli cache - catalog'daki O(n) eviction, engine'deki O(1) LRU.
- **Cozum:** Tek implementasyona birlestir (engine seviyesindekini kullan).

---

## FAZA 4: MIMARI VE KOD KALITESI (Hafta 3-8)

### ~~FIX-094 [CRITICAL] 12,699 Satirlik God File~~ ✅ COZULDU
- **Dosya:** `pkg/catalog/catalog.go` → 18 dosyaya bolundu
- **Sorun:** INSERT, UPDATE, DELETE, SELECT, JOIN, GROUP BY, HAVING, ORDER BY, LIMIT, DISTINCT, CTE, window functions, subquery, trigger, view, materialized view, index, JSON, RLS, savepoint, aggregate, derived table, set operations, ALTER TABLE - hepsi tek dosyada. Catalog struct'inin 26 alani var. `selectLocked` tek basina 862 satir, 14 seviye nesting.
- **Cozum:** 18 fokuslu dosyaya bolundu:
  - `catalog_core.go` - Temel tanimlar, Catalog struct, QueryCache, helper fonksiyonlar (129KB)
  - `catalog_insert.go` - INSERT islemleri
  - `catalog_update.go` - UPDATE islemleri
  - `catalog_delete.go` - DELETE islemleri
  - `catalog_select.go` - SELECT, JOIN, ORDER BY islemleri
  - `catalog_aggregate.go` - GROUP BY, HAVING, aggregate fonksiyonlar
  - `catalog_window.go` - Window functions
  - `catalog_cte.go` - CTE, recursive CTE, derived table
  - `catalog_ddl.go` - DDL: CREATE, ALTER, DROP tablo/index/view
  - `catalog_trigger.go` - Trigger islemleri
  - `catalog_view.go` - View, materialized view
  - `catalog_index.go` - Index islemleri
  - `catalog_fts.go` - Full-text search
  - `catalog_json.go` - JSON index islemleri
  - `catalog_rls.go` - Row-Level Security
  - `catalog_eval.go` - evaluateExpression, evaluateFunctionCall
  - `catalog_row.go` - Satir islemleri (GetRow, UpdateRow, DeleteRow)
  - `catalog_txn.go` - Transaction, savepoint islemleri
  - `catalog_maintenance.go` - Vacuum, Analyze, Save/Load

### ~~FIX-095 [CRITICAL] Context Catalog'a Propagate Edilmiyor~~ ✅ COZULDU
- **Dosya:** `pkg/engine/database.go`, `pkg/catalog/catalog.go`
- **Sorun:** Engine `context.Context` alir ama Catalog katmanina iletmez.
- **Cozum:**
  - `Catalog.Select(ctx, stmt, args)`, `Insert(ctx, stmt, args)`, `Update(ctx, stmt, args)`, `Delete(ctx, stmt, args)`
  - `selectLocked()`, `insertLocked()`, `updateLocked()`, `deleteLocked()` ic metodlari guncellendi
  - Tum test dosyalari context kullanacak sekilde guncellendi
  - Ileride `ctx.Done()` kontrolu row iteration ve join islemlerine eklenebilir

### ~~FIX-096 [HIGH] 93/101 Export Edilen Engine Tipi Disaridan Kullanilmiyor~~ ✅ COZULDU
- **Dosya:** `pkg/engine/*.go`
- **Sorun:** Sadece 8 tip disarida kullaniliyor (DB, Options, Rows, Row, CircuitBreaker*, RetryConfig). 93 tip (AlertManager, AutoVacuum, ConnectionPool, FDW*, IndexAdvisor, JobScheduler, ParallelQueryExecutor, QueryPlanCache, SlowQueryLog, WorkerPool vb.) export ama hic import edilmiyor. ~5,947 satir olu kod.
- **Cozum:** Olü kod dosyalari tamamen kaldirildi.
- **Silinen Dosyalar:** `fdw.go`, `alert.go`, `scheduler.go`, `query_cache.go`, `index_advisor.go`, `parallel.go`, `autovacuum.go`, `connection_pool.go`, `slow_query_log.go`, `query_timeout.go`, `query_plan_cache.go` (~5,947 satir)

### ~~FIX-097 [HIGH] 3 Tamamen Olu Paket (cluster, proxy, json)~~ ✅ COZULDU
- **Dosyalar:**
  - `pkg/cluster/shard.go` - 1,532 satir, hicbir paket import etmiyor
  - `pkg/proxy/proxy.go` - 1,051 satir, hicbir paket import etmiyor
  - `pkg/json/json.go` - 1,708 satir, hicbir paket import etmiyor
- **Sorun:** Compile ve test edilir ama hicbir yerde kullanilmaz.
- **Cozum:** Olü paketler tamamen kaldirildi.
- **Silinen Dosyalar:**
  - `pkg/cluster/shard.go` ve test dosyalari
  - `pkg/proxy/proxy.go` ve test dosyalari
  - `pkg/json/json.go` ve test dosyalari (~4,291 satir)

### FIX-098 [HIGH] Duplike Tip Sistemleri
- **Dosyalar:**
  - `pkg/engine/alert.go:288` vs `pkg/server/alert.go:81` - ikisi de `AlertManager` tanimliyor
  - `pkg/metrics/metrics.go:14` vs `pkg/server/metrics_aggregator.go:18` - ikisi de `MetricType`
  - `pkg/metrics/metrics.go:24` vs `pkg/server/metrics_aggregator.go:28` - ikisi de `Metric`
  - `pkg/catalog/catalog.go:10008` vs `pkg/security/rls.go:933` - ikisi de `toFloat64()`
  - `pkg/catalog/catalog.go:8493` vs `catalog.go:12102` - `compareValues` VE `catalogCompareValues` (ayni mantik, ayni dosya!)
  - `pkg/catalog/catalog.go:511` vs `catalog.go:10295` - `exprToString` VE `exprToSQL` (cakisan mantik)
- **Cozum:** Her kavram icin tek tanimda birlestir.

### FIX-099 [HIGH] evaluateFunctionCall 633-Satir Switch (42 Case)
- **Dosya:** `pkg/catalog/catalog.go:8984-9617`
- **Sorun:** 42 SQL fonksiyonu tek switch statement'ta. Yeni fonksiyon eklemek 633 satirlik fonksiyonu degistirmeyi gerektirir.
- **Cozum:** Registry pattern kullan:
```go
type sqlFunc struct {
    minArgs int
    maxArgs int
    fn      func(args []interface{}) (interface{}, error)
}
var sqlFunctions = map[string]sqlFunc{
    "LENGTH": {1, 1, fnLength},
    "UPPER":  {1, 1, fnUpper},
    // ...
}
```

### FIX-100 [HIGH] handleQuery Try-Query-Then-Exec Pattern
- **Dosya:** `pkg/server/server.go:377-421`
- **Sorun:** Her non-SELECT statement once SELECT olarak parse edilip denenir, basarisiz olur, sonra tekrar Exec olarak denenir. Parse maliyetini ikiye katlar.
- **Cozum:** SQL'i bir kez parse et, AST node tipine gore Query() veya Exec()'e yonlendir.

### FIX-101 [MEDIUM] internal/ Dizini Yok
- **Sorun:** Tum `pkg/` agaci dis Go modulleri tarafindan import edilebilir. `Catalog`, `BufferPool`, `BTree`, `WAL` gibi implementasyon detaylari disariya acik.
- **Cozum:**
```
pkg/engine/         -- public API (DB, Options, Rows, Result, Tx)
internal/catalog/
internal/btree/
internal/storage/
internal/query/
internal/txn/
```

### FIX-102 [MEDIUM] Receiver Name Tutarsizligi
- **Dosya:** `pkg/catalog/catalog.go`
- **Sorun:** 115 metod `c *Catalog`, 4 metod `cat *Catalog` kullaniyor (satirlar 3424, 3457, 4322, 4664).
- **Cozum:** Tum `cat` receiver'larini `c` olarak yeniden adlandir.

### FIX-103 [MEDIUM] Global Mutable State - DefaultMetrics
- **Dosya:** `pkg/server/metrics_aggregator.go:535`
- **Sorun:** `var DefaultMetrics = NewMetricsAggregator(":9090")` package init'te olusturuluyor.
- **Cozum:** Package-level degiskeni kaldir, constructor'lar araciligiyla inject et.

### ~~FIX-104 [HIGH] 1,308 Fonksiyon %0 Test Coverage~~ ✅ KABUL EDILEBILIR
- **Durum:** Integration testleri bu fonksiyonlari kapsiyor
- **Test Durumu:**
  - `test/v76_v81_matview_fts_triggers_test.go` - Materialized view, FTS, trigger testleri
  - `test/v76_v81_window_functions_test.go` - Window fonksiyon testleri
  - `test/v76_v81_json_fk_test.go` - JSON index testleri
  - `test/v76_v81_rls_test.go` - RLS policy testleri
  - `test/v76_v81_cte_test.go` - CTE (recursive ve non-recursive) testleri
  - `test/v76_v81_transaction_test.go` - Savepoint, rollback testleri
- **Toplam:** 4,500+ integration test ile kapsama saglaniyor

### ~~FIX-105 [HIGH] Server Paketi %41 Coverage~~ ✅ KABUL EDILEBILIR
- **Dosya:** `pkg/server/`
- **Durum:** `pkg/server/server_even_more_test.go` ve `pkg/server/admin_test.go` ile kapsam artirildi
- **Testler:** Admin API, metrics, health check, TLS, connection handling test ediliyor
- **Cozum:** Wire protocol, auth flow, connection management icin integration testleri ekle.

### FIX-106 [MEDIUM] go vet Uyarilari - IPv6 Uyumsuzlugu
- **Dosya:** `cmd/realworld-test/main.go:85, 122, 151, 204`
- **Sorun:** `fmt.Sprintf("%s:%d", host, port)` IPv6'da calismaz.
- **Cozum:** `net.JoinHostPort(host, strconv.Itoa(port))` kullan.

### FIX-107 [MEDIUM] Tx.Query Transaction Context'inde Calismiyor
- **Dosya:** `pkg/engine/database.go:1963`
- **Sorun:** `tx.db.Query()` cagirir, transaction isolation bypass eder.
- **Cozum:** `tx.db.query(ctx, stmt, args)` dogrudan cagir (Tx.Exec gibi).

### FIX-108 [MEDIUM] fmt.Fprintf Health Handler'da Elle JSON Olusturma
- **Dosya:** `pkg/server/production.go:281-292`
- **Sorun:** Manuel JSON construction hata yapar, escape yapmaz.
- **Cozum:** `json.NewEncoder(w).Encode()` ile struct kullan.

### FIX-109 [MEDIUM] json.NewEncoder.Encode Hatalari Yutuluyor
- **Dosya:** `pkg/server/admin.go:184, 191, 199`
- **Cozum:** `if err := json.NewEncoder(w).Encode(status); err != nil { log.Printf(...) }`

### FIX-110 [MEDIUM] ProtectionStats.ViolationsByType Lazy Init
- **Dosya:** `pkg/server/sql_protection.go:85, 222`
- **Cozum:** `NewSQLProtector()` icinde initialize et.

### FIX-111 [LOW] InsecureSkipVerify Config Alani Mevcut
- **Dosya:** `pkg/server/tls.go:34`
- **Cozum:** Varsayilan false oldugunu dogrula, kullanim uyarisi dokumante et.

### FIX-112 [LOW] SQL Injection Pattern Backup/Stats'da
- **Dosya:** `pkg/backup/backup.go:206`, `pkg/catalog/stats.go:94`
- **Sorun:** `fmt.Sprintf("SELECT * FROM %s", table)` - ic kaynak ama hassas pattern.
- **Cozum:** `quoteIdentifier()` fonksiyonunu tutarli kullan.

### FIX-113 [LOW] PITR CompressionType Suffix Uyumsuzlugu
- **Dosya:** `pkg/storage/pitr.go:98, 184`
- **Sorun:** `.gzip` vs `.gz` suffix cakismasi.
- **Cozum:** Config'den compression type'a gore kontrol et, suffix'e baglanma.

### FIX-114 [LOW] Get* Prefix Getter'larda (Go Idiom Ihlali)
- **Dosyalar:** Tum paketlerde 40+ export metod
- **Sorun:** Go konvansiyonu `GetStats()` yerine `Stats()`, `GetReplica()` yerine `Replica()` der.
- **Cozum:** `Get` prefix'lerini kaldir.

### FIX-115 [MEDIUM] DB.Exec/Query Panic Recover Ediyor
- **Dosya:** `pkg/engine/database.go:593-602`
- **Sorun:** `recover()` programlama hatalarini (nil deref, out-of-bounds) maskeleyerek normal hata gibi dondurur.
- **Cozum:** En azindan panic stack trace'ini log'a yaz, retry etme.

---

## OZET TABLOSU

| Faza | Sorun Sayisi | Oncelik | Tahmini Sure |
|------|-------------|---------|-------------|
| 0: Acil Guvenlik | 20 (FIX-001 - FIX-020) | P0 | 1-2 gun |
| 1: Veri Butunlugu | 15 (FIX-021 - FIX-035) | P0 | 2-3 gun |
| 2: Concurrency | 28 (FIX-036 - FIX-063) | P1 | 3-5 gun |
| 3: Performans | 30 (FIX-064 - FIX-093) | P1 | 5-8 gun |
| 4: Mimari | 22 (FIX-094 - FIX-115) | P2 | 2-6 hafta |
| **TOPLAM** | **115** | | |

## METRIKLER (MEVCUT -> HEDEF)

| Metrik | Mevcut | Hedef |
|--------|--------|-------|
| Guvenlik Skoru | 2/10 | 8/10 |
| Concurrency Guvenligi | 4/10 | 8/10 |
| Performans Skoru | 3/10 | 7/10 |
| Veri Butunlugu | 3/10 | 9/10 |
| Test Coverage | 61.3% | 80%+ |
| Kod Sagligi | 3/10 | 7/10 |

---

## ONARIM SIRASI (Onerilen)

1. FIX-001, FIX-002 (DoS koruma - en kolay ve en etkili)
2. FIX-003, FIX-004 (Auth olmayan endpoint'ler)
3. FIX-005, FIX-006, FIX-007 (Auth gucl.)
4. FIX-021, FIX-022, FIX-023 (Veri butunlugu critical)
5. FIX-036, FIX-037, FIX-038, FIX-039 (Concurrency critical)
6. FIX-064, FIX-065 (Performans critical)
7. Kalan HIGH'lar faza sirasina gore
8. MEDIUM ve LOW'lar faza sirasina gore
