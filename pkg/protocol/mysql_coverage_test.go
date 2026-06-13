package protocol

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/auth"
)

func TestSetAuthenticator(t *testing.T) {
	server := NewMySQLServer(nil, "")
	if server.auth != nil {
		t.Error("auth should be nil initially")
	}

	a := auth.NewAuthenticator()
	server.SetAuthenticator(a)
	if server.auth == nil {
		t.Error("auth should be set")
	}
}

func TestSetAllowCleartextAuth(t *testing.T) {
	server := NewMySQLServer(nil, "")
	if server.allowCleartextAuth {
		t.Fatal("cleartext auth should be disabled by default")
	}

	server.SetAllowCleartextAuth(true)
	if !server.allowCleartextAuth {
		t.Fatal("cleartext auth should be enabled after setter")
	}
}

func TestValidateMySQLAuthTransport(t *testing.T) {
	tests := []struct {
		name               string
		address            string
		authEnabled        bool
		allowCleartextAuth bool
		wantErr            bool
	}{
		{
			name:        "auth wildcard rejected",
			address:     "0.0.0.0:3306",
			authEnabled: true,
			wantErr:     true,
		},
		{
			name:        "auth empty host rejected",
			address:     ":3306",
			authEnabled: true,
			wantErr:     true,
		},
		{
			name:        "auth loopback allowed",
			address:     "127.0.0.1:3306",
			authEnabled: true,
		},
		{
			name:        "auth localhost allowed",
			address:     "localhost:3306",
			authEnabled: true,
		},
		{
			name:               "explicit cleartext allowed",
			address:            "0.0.0.0:3306",
			authEnabled:        true,
			allowCleartextAuth: true,
		},
		{
			name:    "auth disabled allowed",
			address: "0.0.0.0:3306",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMySQLAuthTransport(tt.address, tt.authEnabled, tt.allowCleartextAuth)
			if tt.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected validation to pass, got %v", err)
			}
		})
	}
}

func TestMySQLListenRejectsAuthenticatedNonLoopbackCleartext(t *testing.T) {
	server := NewMySQLServer(nil, "")
	a := auth.NewAuthenticator()
	a.Enable()
	server.SetAuthenticator(a)

	err := server.Listen("0.0.0.0:0")
	if err == nil {
		t.Fatal("expected Listen to reject authenticated non-loopback cleartext address")
	}
	if !strings.Contains(err.Error(), "MySQL authentication on non-loopback") {
		t.Fatalf("expected auth transport error, got %v", err)
	}
	if server.listener != nil {
		t.Fatal("listener should not be opened after auth transport rejection")
	}
}

func TestMySQLServerConcurrentConfigAccess(t *testing.T) {
	server := NewMySQLServer(nil, "")
	a := auth.NewAuthenticator()
	a.Enable()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(4)
		go func(i int) {
			defer wg.Done()
			server.SetAuthenticator(a)
		}(i)
		go func(i int) {
			defer wg.Done()
			server.SetAllowCleartextAuth(i%2 == 0)
		}(i)
		go func() {
			defer wg.Done()
			_ = server.authEnabled()
		}()
		go func() {
			defer wg.Done()
			_ = server.Addr()
		}()
	}
	wg.Wait()
}

func TestAddrNilListener(t *testing.T) {
	server := NewMySQLServer(nil, "")
	addr := server.Addr()
	if addr != nil {
		t.Error("expected nil addr when listener is nil")
	}
}

func TestSanitizeMySQLError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected string
	}{
		{
			name:     "no path",
			err:      errors.New("column not found"),
			expected: "column not found",
		},
		{
			name:     "unix path",
			err:      errors.New("error opening /var/data/file.db: permission denied"),
			expected: "error opening (internal error)",
		},
		{
			name:     "windows C path",
			err:      errors.New("error opening C:\\Users\\data\\file.db: permission denied"),
			expected: "error opening (internal error)",
		},
		{
			name:     "windows D path",
			err:      errors.New("error at D:\\Codebox\\cobaltdb\\test"),
			expected: "error at (internal error)",
		},
		{
			name:     "empty error",
			err:      errors.New(""),
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeMySQLError(tt.err)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestWriteLenEncIntCoverage(t *testing.T) {
	tests := []struct {
		name     string
		value    uint64
		minBytes int
		maxBytes int
	}{
		{"zero", 0, 1, 1},
		{"small", 100, 1, 1},
		{"250", 250, 1, 1},
		{"251", 251, 3, 3},         // 0xFC + 2 bytes
		{"65535", 65535, 3, 3},     // 0xFC + 2 bytes
		{"65536", 65536, 4, 4},     // 0xFD + 3 bytes
		{"large", 1<<24 + 1, 9, 9}, // 0xFE + 8 bytes
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := appendLenEncInt(nil, tt.value)
			if len(result) < tt.minBytes || len(result) > tt.maxBytes {
				t.Errorf("value %d: expected %d-%d bytes, got %d", tt.value, tt.minBytes, tt.maxBytes, len(result))
			}
		})
	}
}

func TestWriteLenEncString(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"hello", "hello"},
		{"unicode", "日本語"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := writeLenEncString(tt.input)
			if result == nil {
				t.Error("expected non-nil result")
			}
			// Result should have length prefix + string bytes
			if len(result) < len(tt.input) {
				t.Errorf("result too short: %d < %d", len(result), len(tt.input))
			}
		})
	}
}
