package protocol

import (
	"errors"
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
			result := writeLenEncInt(tt.value)
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
