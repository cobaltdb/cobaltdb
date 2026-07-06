package audit

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func TestIsAuditHash(t *testing.T) {
	tests := []struct {
		name  string
		hash  string
		want  bool
	}{
		{
			name:  "valid 64-char hex lowercase",
			hash:  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			want:  true,
		},
		{
			name:  "valid 64-char hex mixed case is rejected (only a-f)",
			hash:  "ABCDEF0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
			want:  false,
		},
		{
			name:  "too short",
			hash:  "abc",
			want:  false,
		},
		{
			name:  "too long",
			hash:  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789ff",
			want:  false,
		},
		{
			name:  "contains 'g' (outside hex range)",
			hash:  "abcdef0123456789abcdef0123456789abcdef0123456789abcdef012345678g",
			want:  false,
		},
		{
			name:  "contains uppercase G",
			hash:  "bcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789G",
			want:  false,
		},
		{
			name:  "empty string",
			hash:  "",
			want:  false,
		},
		{
			name:  "exactly sha256.Size*2 hex chars",
			hash:  hex.EncodeToString(make([]byte, sha256.Size)),
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isAuditHash(tt.hash)
			if got != tt.want {
				t.Errorf("isAuditHash(%q) = %v, want %v", tt.hash, got, tt.want)
			}
		})
	}
}

func TestExtractTextAuditHash(t *testing.T) {
	validHash := "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	tests := []struct {
		name     string
		line     string
		wantHash string
		wantOK   bool
	}{
		{
			name:     "line with hash at end",
			line:     "[2024-01-01T00:00:00Z] QUERY evt_001 user=alice action=SELECT status=OK hash=" + validHash,
			wantHash: validHash,
			wantOK:   true,
		},
		{
			name:     "line with prev_hash and hash",
			line:     "[2024-01-01T00:00:00Z] QUERY evt_001 user=alice action=SELECT status=OK prev_hash=aaa hash=" + validHash,
			wantHash: validHash,
			wantOK:   true,
		},
		{
			name:     "no hash marker",
			line:     "[2024-01-01T00:00:00Z] QUERY evt_001 user=alice action=SELECT status=OK",
			wantHash: "",
			wantOK:   false,
		},
		{
			name:     "empty string",
			line:     "",
			wantHash: "",
			wantOK:   false,
		},
		{
			name:     "hash value is not valid hex",
			line:     "[2024-01-01T00:00:00Z] QUERY evt_001 user=alice action=SELECT status=OK hash=nothex",
			wantHash: "",
			wantOK:   false,
		},
		{
			name:     "hash value too short",
			line:     "[2024-01-01T00:00:00Z] QUERY evt_001 user=alice action=SELECT status=OK hash=abc",
			wantHash: "",
			wantOK:   false,
		},
		{
			name:     "hash appears mid-line (not last)",
			line:     "hash=" + validHash + " extra=data",
			wantHash: "",
			wantOK:   false,
		},
		{
			name:     "trailing spaces after hash",
			line:     "[2024-01-01T00:00:00Z] QUERY evt_001 user=alice action=SELECT status=OK hash=" + validHash + "  ",
			wantHash: validHash,
			wantOK:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotHash, gotOK := extractTextAuditHash(tt.line)
			if gotHash != tt.wantHash || gotOK != tt.wantOK {
				t.Errorf("extractTextAuditHash(%q) = (%q, %v), want (%q, %v)",
					tt.line, gotHash, gotOK, tt.wantHash, tt.wantOK)
			}
		})
	}
}

func TestReadLastTextAuditHash(t *testing.T) {
	validHash1 := "1111111111111111111111111111111111111111111111111111111111111111"
	validHash2 := "2222222222222222222222222222222222222222222222222222222222222222"

	t.Run("empty file returns empty string", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")
		if err := os.WriteFile(path, []byte{}, 0600); err != nil {
			t.Fatal(err)
		}
		hash, err := readLastTextAuditHash(path, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != "" {
			t.Errorf("got hash %q, want empty", hash)
		}
	})

	t.Run("single line returns that hash", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")
		line := "user=alice action=SELECT status=OK hash=" + validHash1 + "\n"
		if err := os.WriteFile(path, []byte(line), 0600); err != nil {
			t.Fatal(err)
		}
		hash, err := readLastTextAuditHash(path, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != validHash1 {
			t.Errorf("got hash %q, want %q", hash, validHash1)
		}
	})

	t.Run("multiple lines returns last hash", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")
		content := "user=alice action=SELECT status=OK hash=" + validHash1 + "\n" +
			"user=bob action=INSERT status=OK hash=" + validHash2 + "\n"
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		hash, err := readLastTextAuditHash(path, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != validHash2 {
			t.Errorf("got hash %q, want %q", hash, validHash2)
		}
	})

	t.Run("skips blank lines", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")
		content := "\n\nuser=alice action=SELECT status=OK hash=" + validHash1 + "\n\n"
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		hash, err := readLastTextAuditHash(path, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != validHash1 {
			t.Errorf("got hash %q, want %q", hash, validHash1)
		}
	})

	t.Run("line without hash returns error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")
		line := "user=alice action=SELECT status=OK\n"
		if err := os.WriteFile(path, []byte(line), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := readLastTextAuditHash(path, nil)
		if err == nil {
			t.Fatal("expected error for line without hash")
		}
	})

	t.Run("invalid hash returns error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")
		line := "user=alice action=SELECT status=OK hash=invalid\n"
		if err := os.WriteFile(path, []byte(line), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := readLastTextAuditHash(path, nil)
		if err == nil {
			t.Fatal("expected error for invalid hash")
		}
	})

	t.Run("encrypted lines with AEAD", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")

		// Set up AES-GCM
		key := make([]byte, 32)
		for i := range key {
			key[i] = byte(i)
		}
		block, err := aes.NewCipher(key)
		if err != nil {
			t.Fatal(err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			t.Fatal(err)
		}

		// Manually encrypt a line with hash (use base64, matching decryptAuditLogLine)
		plaintext := "user=alice action=SELECT status=OK hash=" + validHash1
		nonce := make([]byte, aead.NonceSize())
		ciphertext := aead.Seal(nonce, nonce, []byte(plaintext), nil)
		encoded := "ENC:" + base64.StdEncoding.EncodeToString(ciphertext) + "\n"

		if err := os.WriteFile(path, []byte(encoded), 0600); err != nil {
			t.Fatal(err)
		}

		hash, err := readLastTextAuditHash(path, aead)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if hash != validHash1 {
			t.Errorf("got hash %q, want %q", hash, validHash1)
		}
	})

	t.Run("encrypted line without AEAD returns error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "audit.log")
		line := "ENC:someciphertext\n"
		if err := os.WriteFile(path, []byte(line), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := readLastTextAuditHash(path, nil)
		if err == nil {
			t.Fatal("expected error for encrypted line without key")
		}
	})

	t.Run("non-existent file returns error", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "nonexistent.log")
		_, err := readLastTextAuditHash(path, nil)
		if err == nil {
			t.Fatal("expected error for non-existent file")
		}
	})
}
