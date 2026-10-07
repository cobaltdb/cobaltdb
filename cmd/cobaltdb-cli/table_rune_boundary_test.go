package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

// captureStdout redirects os.Stdout while fn runs.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	fn()
	_ = w.Close()
	os.Stdout = old
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(out)
}

// multibyteTruncationCell builds a cell of n ASCII bytes then a 3-byte rune then filler, so
// that a byte-index slice near the cap lands INSIDE the multi-byte rune.
func multibyteTruncationCell() string {
	return strings.Repeat("a", 56) + "€" + strings.Repeat("b", 10) // 69 bytes; € occupies bytes 56,57,58
}

func TestTableTruncationKeepsValidUTF8(t *testing.T) {
	cell := multibyteTruncationCell()
	widths := []int{60} // the production cap from printRowsTable
	out := captureStdout(t, func() { printTableRow([]string{cell}, widths) })

	// CONTROL 1: long ASCII cell truncates exactly as before.
	ascii := strings.Repeat("x", 100)
	asciiOut := captureStdout(t, func() { printTableRow([]string{ascii}, []int{60}) })
	if !strings.Contains(asciiOut, strings.Repeat("x", 57)+"...") {
		t.Errorf("control: ASCII truncation changed, got %q", asciiOut)
	}
	// CONTROL 2: short ASCII cell is untouched (no truncation, padded).
	shortOut := captureStdout(t, func() { printTableRow([]string{"id"}, []int{5}) })
	if !strings.Contains(shortOut, "id    │") {
		t.Errorf("control: short ASCII cell rendering changed, got %q", shortOut)
	}

	if !utf8.ValidString(out) {
		t.Fatalf("FAIL: printTableRow emitted INVALID UTF-8 for a valid UTF-8 input.\n got %q\n bytes %x", out, []byte(out))
	}
	if strings.ContainsRune(out, utf8.RuneError) {
		t.Fatalf("FAIL: printTableRow emitted a replacement rune (mojibake). got %q", out)
	}
	t.Logf("PASS: valid UTF-8: %q", out)
}
