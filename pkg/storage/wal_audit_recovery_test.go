package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
)

func TestWALRecoveryReusedTransactionIDRequiresNewCommit(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, physical := range []bool{false, true} {
			for _, firstCommit := range []WALRecordType{WALCommit, WALUpdateCommit} {
				for _, ending := range []string{"eof", "rollback", "commit", "combined", "rollback_then_commit"} {
					name := fmt.Sprintf("encrypted=%v/physical=%v/first=%d/%s", encrypted, physical, firstCommit, ending)
					t.Run(name, func(t *testing.T) {
						path := filepath.Join(t.TempDir(), "reused.wal")
						bp := NewBufferPool(4, NewMemory())
						defer bp.Close()
						var pageID uint32
						if physical {
							page, err := bp.NewPage(PageTypeLeaf)
							if err != nil {
								t.Fatal(err)
							}
							pageID = page.ID()
							bp.Unpin(page)
							if err := bp.FlushAll(); err != nil {
								t.Fatal(err)
							}
						}
						c := makeTestCipher(t)
						open := func() *WAL {
							w, err := OpenWAL(path)
							if err != nil {
								t.Fatal(err)
							}
							if encrypted {
								w.SetEncryptionCipher(c)
							}
							return w
						}
						write := func(w *WAL, kind WALRecordType, offset uint16, value string) {
							r := &WALRecord{TxnID: 1, Type: kind, Data: []byte(value)}
							if physical {
								r.PageID = pageID
								r.Offset = offset
							}
							if err := w.Append(r); err != nil {
								t.Fatal(err)
							}
						}
						w := open()
						if firstCommit == WALCommit {
							write(w, WALUpdate, 100, "first")
							write(w, WALCommit, 0, "")
						} else {
							write(w, WALUpdateCommit, 100, "first")
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
						// Simulate a fresh transaction counter after reopening.
						w = open()
						write(w, WALUpdate, 200, "second")
						want := []string{"first"}
						switch ending {
						case "rollback":
							write(w, WALRollback, 0, "")
						case "commit":
							write(w, WALCommit, 0, "")
							want = append(want, "second")
						case "combined":
							write(w, WALUpdateCommit, 300, "third")
							want = append(want, "second", "third")
						case "rollback_then_commit":
							write(w, WALRollback, 0, "")
							write(w, WALUpdate, 300, "third")
							write(w, WALCommit, 0, "")
							want = append(want, "third")
						}
						if err := w.Close(); err != nil {
							t.Fatal(err)
						}
						w = open()
						defer w.Close()
						for pass := 0; pass < 2; pass++ {
							if err := w.Recover(bp); err != nil {
								t.Fatal(err)
							}
							if physical {
								page, err := bp.GetPage(pageID)
								if err != nil {
									t.Fatal(err)
								}
								actual := append([]byte(nil), page.Data()...)
								bp.Unpin(page)
								expected := make([]byte, len(actual)-100)
								copy(expected, "first")
								for _, value := range want[1:] {
									offset := 100
									if value == "third" {
										offset = 200
									}
									copy(expected[offset:], value)
								}
								if !bytes.Equal(actual[100:], expected) {
									t.Fatalf("recovery pass %d: physical page differs from committed writes %q", pass, want)
								}
							} else {
								var actual []string
								for _, op := range w.GetReplayOps() {
									actual = append(actual, string(op.Data))
								}
								if !reflect.DeepEqual(actual, want) {
									t.Fatalf("recovery pass %d: got %q, want %q", pass, actual, want)
								}
							}
						}
					})
				}
			}
		}
	}
}
