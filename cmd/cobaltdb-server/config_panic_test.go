package main

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadConfigFileMalformedValueDoesNotPanic pins a startup-robustness
// contract: malformed value lines in the optional config file (missing value,
// lone quote) must be skipped or errored — never panic. The quote-stripping
// condition in loadConfigFile evaluated the single-quote disjunct without the
// length guard, so `key =` indexed an empty string and `key = '` sliced out
// of range, crashing server startup with a panic instead of a clean error.
func TestLoadConfigFileMalformedValueDoesNotPanic(t *testing.T) {
	cases := []struct {
		name string
		line string
	}{
		{"missing value", "port ="},
		{"empty value with spaces", "host =   "},
		{"lone single quote", "log_format = '"},
		{"lone double quote", "log_format = \""},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cobaltdb.conf")
			if err := os.WriteFile(path, []byte("[server]\n"+tc.line+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("loadConfigFile panicked on malformed line %q: %v", tc.line, r)
				}
			}()
			cv, err := loadConfigFile(path)
			if err != nil {
				t.Fatalf("loadConfigFile returned error for malformed line %q: %v", tc.line, err)
			}
			if cv == nil {
				t.Fatal("loadConfigFile returned nil values")
			}
		})
	}
}

// TestLoadConfigFileValidValuesIsControl pins the happy path so the panic fix
// cannot regress normal parsing: quoted values (both quote kinds) are
// stripped, durations parse to seconds, and host+port compose into addresses.
func TestLoadConfigFileValidValuesIsControl(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cobaltdb.conf")
	content := "[server]\n" +
		"host = 127.0.0.1\n" +
		"port = 4200\n" +
		"connection_timeout = 30s\n" +
		"[storage]\n" +
		"data_dir = 'data'\n" +
		"cache_size = 512\n" +
		"[security]\n" +
		"auth_enabled = true\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	cv, err := loadConfigFile(path)
	if err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}
	if cv.DataDir != "data" {
		t.Fatalf("single-quoted data_dir not stripped: %q", cv.DataDir)
	}
	if cv.ReadTimeout != 30 {
		t.Fatalf("connection_timeout not parsed: %d", cv.ReadTimeout)
	}
	if cv.CacheSize != 512 {
		t.Fatalf("cache_size not parsed: %d", cv.CacheSize)
	}
	if cv.AuthEnabled == nil || !*cv.AuthEnabled {
		t.Fatalf("auth_enabled not parsed: %v", cv.AuthEnabled)
	}
	if cv.Address != "127.0.0.1:4200" {
		t.Fatalf("host+port not composed: %q", cv.Address)
	}

	path2 := filepath.Join(t.TempDir(), "cobaltdb.conf")
	content2 := "[server]\nhost = 0.0.0.0\nport = 4300\n"
	if err := os.WriteFile(path2, []byte(content2), 0600); err != nil {
		t.Fatal(err)
	}
	cv2, err := loadConfigFile(path2)
	if err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}
	if cv2.Address != "0.0.0.0:4300" {
		t.Fatalf("host+port not composed: %q", cv2.Address)
	}
}
