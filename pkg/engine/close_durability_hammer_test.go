package engine

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCloseDurabilityHammer guards the Close/in-flight-write lifecycle on a
// real disk-backed WAL-enabled database across a Close:
//
//	acknowledged (Exec returned nil)  -> row MUST exist after reopen
//	rejected     (Exec returned err)  -> row MUST NOT exist after reopen
//
// History: Close() tears down subsystems (query cache disable, WAL
// checkpoint/close, buffer pool flush/close) underneath statements it already
// admitted — execution runs outside db.mu (the closed check happens under a
// short parse-time RLock). That produced a data race between
// Catalog.DisableQueryCache (c.mu-held write) and in-flight
// invalidateQueryCache readers (unsynchronized c.queryCache loads) — fixed by
// making the field atomic.Pointer[cache.Cache] — and risks undefined
// mid-teardown statement outcomes. This hammer failed pre-fix with
// "WARNING: DATA RACE" under -race; run this test with -race in CI.
//
// Any single violation of either contract class is a regression.
func TestCloseDurabilityHammer(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hammer.db")

	opts := &Options{}
	walOn := true
	opts.CoreStorage.WALEnabled = &walOn

	db, err := Open(path, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.Exec(nil, "CREATE TABLE hammer_round6 (k INTEGER PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatalf("create: %v", err)
	}

	const writers = 4
	const perWriter = 200000

	var (
		mu          sync.Mutex
		ack         = map[string]bool{}
		rejected    = map[string]string{} // key -> error class
		ackCount    atomic.Int64
		dirtyErrors atomic.Int64
		cleanStops  atomic.Int64
	)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			base := (w + 1) * 1000000
			for i := 1; i <= perWriter; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("%d", base+i)
				_, execErr := db.Exec(nil, "INSERT INTO hammer_round6 VALUES (?, ?)", base+i, fmt.Sprintf("v%d", i))
				mu.Lock()
				switch {
				case execErr == nil:
					ack[k] = true
					ackCount.Add(1)
				case errors.Is(execErr, ErrDatabaseClosed):
					rejected[k] = "closed"
					cleanStops.Add(1)
					mu.Unlock()
					return // clean post-close rejection: writer stops
				default:
					rejected[k] = execErr.Error()
					dirtyErrors.Add(1)
				}
				mu.Unlock()
			}
		}(w)
	}

	// Wait until the writers are demonstrably acknowledged-and-running.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if ackCount.Load() >= 1 {
			break
		}
		if time.Now().After(deadline) {
			close(stop)
			t.Fatal("setup: no acknowledged writes observed before Close")
		}
		time.Sleep(1 * time.Millisecond)
	}

	if closeErr := db.Close(); closeErr != nil {
		t.Fatalf("Close returned error: %v", closeErr)
	}
	close(stop)

	wgDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(wgDone)
	}()
	select {
	case <-wgDone:
	case <-time.After(30 * time.Second):
		t.Fatal("writer goroutines did not terminate within 30s of Close returning — in-flight work leaked past Close")
	}

	// Reopen and collect what actually survived.
	db2, err := Open(path, opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	rows, err := db2.Query(nil, "SELECT k FROM hammer_round6")
	if err != nil {
		t.Fatalf("query after reopen: %v", err)
	}
	survived := map[string]bool{}
	for _, row := range rows.rows {
		if len(row) > 0 {
			survived[fmt.Sprintf("%v", row[0])] = true
		}
	}

	var lost, resurrected []string
	for k := range ack {
		if !survived[k] {
			lost = append(lost, k)
		}
	}
	for k := range rejected {
		if survived[k] {
			resurrected = append(resurrected, k)
		}
	}

	t.Logf("hammer: acknowledged=%d cleanStops=%d dirtyErrors=%d survived=%d lost=%d resurrected=%d",
		ackCount.Load(), cleanStops.Load(), dirtyErrors.Load(), len(survived), len(lost), len(resurrected))

	if len(lost) > 0 {
		t.Fatalf("%d acknowledged write(s) lost across Close (e.g. keys %v): durability contract violated",
			len(lost), firstStr(lost, 5))
	}
	if len(resurrected) > 0 {
		t.Fatalf("%d rejected write(s) resurrected after reopen (e.g. keys %v): all-or-nothing contract violated",
			len(resurrected), firstStr(resurrected, 5))
	}
}

func firstStr(xs []string, n int) []string {
	if len(xs) < n {
		n = len(xs)
	}
	return xs[:n]
}
