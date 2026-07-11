package cobaltdb

import (
	"strings"
	"testing"
)

func TestToLowerFast(t *testing.T) {
	if got := toLowerFast("MIXED"); got != "mixed" {
		t.Fatalf("toLowerFast(MIXED) = %q, want mixed", got)
	}
	if got := toLowerFast("lower"); got != "lower" {
		t.Fatalf("toLowerFast(lower) = %q, want lower", got)
	}
	if got := toLowerFast("123"); got != "123" {
		t.Fatalf("toLowerFast(123) = %q, want 123", got)
	}
}

func TestParsePortReportsErrors(t *testing.T) {
	if _, err := parsePort("not-a-number"); err == nil {
		t.Fatal("parsePort should fail for non-numeric")
	}
	if _, err := parsePort("0"); err == nil {
		t.Fatal("parsePort should fail for port 0")
	}
	if _, err := parsePort("65536"); err == nil {
		t.Fatal("parsePort should fail for port > 65535")
	}
}

func TestHasInvalidURLPort(t *testing.T) {
	if hasInvalidURLPort("") {
		t.Fatal("empty host should not have invalid port")
	}
	if !hasInvalidURLPort("host:1234") {
		t.Fatal("host:port should have invalid port (no brackets)")
	}
	// IPv6 without port — Port() would be "", and hasInvalidURLPort
	// must NOT flag the embedded colon as an invalid port specifier.
	if hasInvalidURLPort("[::1]") {
		t.Fatal("IPv6 without port should not be marked as invalid")
	}
	// IPv6 with trailing colon but no port — Port() empty, so
	// hasInvalidURLPort detects the empty port spec.
	if !hasInvalidURLPort("[::1]:") {
		t.Fatal("IPv6 with trailing colon should be marked invalid")
	}
}

func TestParseDSNErrors(t *testing.T) {
	if _, err := ParseDSN(strings.Repeat("x", maxDSNBytes+1)); err == nil {
		t.Fatal("ParseDSN should reject oversized DSN")
	}
}

func TestParseDSNKeyValueErrors(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{"invalid port", "port=notanum"},
		{"invalid connect_timeout", "connect_timeout=notadur"},
		{"invalid query_timeout", "query_timeout=notadur"},
		{"invalid max_conns", "max_conns=notanum"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseDSN(tt.dsn); err == nil {
				t.Fatalf("ParseDSN(%q) should fail", tt.dsn)
			}
		})
	}
}

func TestParseDSNURLFormatErrors(t *testing.T) {
	// Missing port component in host:port format
	if _, err := ParseDSN("cobaltdb://host:"); err == nil {
		t.Fatal("ParseDSN should reject host: without port")
	}
}

func TestConnCloseIsIdempotent(t *testing.T) {
	c := &conn{closed: true}
	if err := c.Close(); err != nil {
		t.Fatalf("Close on already-closed conn should be idempotent: %v", err)
	}
}
