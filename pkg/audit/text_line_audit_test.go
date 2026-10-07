package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTextAuditFieldsRemainOneLine(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, field := range []string{"plain", "action", "user", "status", "event_id"} {
			t.Run(fmt.Sprintf("%t/%s", encrypted, field), func(t *testing.T) {
				cfg := &Config{Enabled: true, LogFile: filepath.Join(t.TempDir(), "audit.log"), LogFormat: "text"}
				if encrypted {
					cfg.EncryptionKey = make([]byte, 32)
				}
				al, err := New(cfg, nil)
				if err != nil {
					t.Fatal(err)
				}
				al.Log(EventAdmin, "system", "maintenance", func(e *Event) {
					switch field {
					case "action":
						e.Action = "maintenance\ncomplete"
					case "user":
						e.User = "first\r\nlast"
					case "status":
						e.Status = "line\rstatus"
					case "event_id":
						e.EventID = "event\nsecond"
					}
					e.Query = "SELECT\n1"
				})
				if err := al.Close(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(cfg.LogFile)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Count(string(data), "\n") != 1 || strings.Contains(string(data), "\r") {
					t.Fatal("record is not one physical line")
				}
				al, err = New(cfg, nil)
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				firstHash := al.lastHash
				if !isAuditHash(firstHash) {
					t.Fatal("invalid first hash")
				}
				al.Log(EventAdmin, "system", "after reopen")
				if err := al.Close(); err != nil {
					t.Fatal(err)
				}
				last, err := readLastTextAuditHash(cfg.LogFile, al.cipher)
				if err != nil || !isAuditHash(last) || last == firstHash {
					t.Fatalf("chain after append: %q %v", last, err)
				}
				data, err = os.ReadFile(cfg.LogFile)
				if err != nil || strings.Count(string(data), "\n") != 2 {
					t.Fatal("append line count", err)
				}
				fmt.Printf("EXPECTED: 2 complete records and successful reopen; ACTUAL: matched (%t/%s)\n", encrypted, field)
			})
		}
	}
	// JSON already escapes string controls and retains its canonical payload.
	cfg := &Config{Enabled: true, LogFile: filepath.Join(t.TempDir(), "json.log"), LogFormat: "json"}
	al, err := New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	al.Log(EventAdmin, "system", "maintenance\ncomplete")
	if err := al.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := VerifyLogFile(cfg.LogFile, nil)
	if err != nil || r.Entries != 1 {
		t.Fatal("JSON control", r, err)
	}
	fmt.Println("FIX VERIFIED")
}
