package txn

import (
	"errors"
	"testing"
)

func TestRecycleActiveTransactionReleasesRegistrationAndLocks(t *testing.T) {
	for _, terminal := range []string{"active", "committed", "aborted", "empty"} {
		t.Run(terminal, func(t *testing.T) {
			m := NewManager(nil)
			for i := 0; i < 20; i++ {
				tx := m.Begin(nil)
				id := tx.ID
				if terminal != "empty" {
					if err := m.AcquireLock(id, "key", 0); err != nil {
						t.Fatal(err)
					}
					tx.SetWrite("table", "key", []byte("pending"))
				}
				if terminal == "committed" {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
				if terminal == "aborted" {
					if err := tx.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				// Gate an observer of the old ID until cleanup and replacement complete.
				gate, observed := make(chan struct{}), make(chan error, 1)
				go func() { <-gate; _, err := m.Get(id); observed <- err }()
				tx.Recycle()
				m.RecycleTxn(tx) // repeated recycle before any reuse
				replacement := m.Begin(nil)
				close(gate)
				if err := <-observed; !errors.Is(err, ErrTxnNotFound) {
					t.Fatalf("old registration: %v", err)
				}
				if err := m.AcquireLock(replacement.ID, "key", 0); err != nil {
					t.Fatal(err)
				}
				if err := replacement.Rollback(); err != nil {
					t.Fatal(err)
				}
				m.lockMu.RLock()
				remaining := len(m.lockEntries)
				m.lockMu.RUnlock()
				if remaining != 0 {
					t.Fatalf("iteration %d: locks=%d", i, remaining)
				}
				if terminal != "committed" && m.GetCurrentVersion("table", "key") != 0 {
					t.Fatal("uncommitted version published")
				}
			}
		})
	}
}
