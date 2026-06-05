package query

import (
	"strings"
	"testing"
)

func TestParseStrictRejectsTrailingTokens(t *testing.T) {
	tests := []string{
		"SELECT * FROM users TABLESAMPLE SYSTEM (10)",
		"SELECT * FROM users MATCH_RECOGNIZE (PARTITION BY id ORDER BY name PATTERN (A))",
		// "SELECT 1 alias" is valid (implicit column alias); a second trailing
		// token after the alias is genuinely unexpected.
		"SELECT 1 alias unexpected",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			if _, err := ParseStrict(sql); err == nil {
				t.Fatal("expected strict parse error")
			} else if !strings.Contains(err.Error(), "unexpected token after statement") {
				t.Fatalf("expected trailing token error, got %v", err)
			}
		})
	}
}

func TestParseStrictAllowsSemicolonTerminator(t *testing.T) {
	if _, err := ParseStrict("SELECT 1;"); err != nil {
		t.Fatalf("ParseStrict with semicolon failed: %v", err)
	}
}

func TestParseStrictRejectsMalformedJoins(t *testing.T) {
	// Malformed cases detected in strict mode by structural anomalies.
	detectablyMalformed := []string{
		"SELECT * FROM a (select 1)", // unclosed paren gives stray )
	}
	for _, sql := range detectablyMalformed {
		t.Run(sql, func(t *testing.T) {
			if _, err := ParseStrict(sql); err == nil {
				t.Fatalf("expected strict parse error: %s", sql)
			}
		})
	}

	// Valid JOINs: strict and permissive must both succeed
	validJoins := []string{
		"SELECT * FROM a JOIN b ON a.id = b.id",
		"SELECT * FROM a LEFT JOIN b ON a.id = b.id",
		"SELECT * FROM a INNER JOIN b ON a.id = b.id",
		"SELECT * FROM a CROSS JOIN b ON a.id = b.id",
		"SELECT * FROM a NATURAL LEFT JOIN b",
		"SELECT * FROM a NATURAL LEFT JOIN b ON a.id = b.id",
	}
	for _, sql := range validJoins {
		t.Run(sql, func(t *testing.T) {
			if _, err := ParseStrict(sql); err != nil {
				t.Fatalf("strict parse should succeed for: %s: %v", sql, err)
			}
			if _, err := Parse(sql); err != nil {
				t.Fatalf("permissive parse should succeed for: %s: %v", sql, err)
			}
		})
	}
}

func TestParsePermissiveAllowsMalformedJoins(t *testing.T) {
	// Permissive mode silently parses malformed JOINs — existing behavior.
	malformed := []string{
		"SELECT * FROM a LEFT b ON a.id = b.id",
		"SELECT * FROM a INNER b ON a.id = b.id",
		"SELECT * FROM a CROSS b ON a.id = b.id",
		"SELECT * FROM a NATURAL LEFT JOIN b ON a.id = b.id",
	}
	for _, sql := range malformed {
		t.Run(sql, func(t *testing.T) {
			stmt, err := Parse(sql)
			if err != nil {
				t.Fatalf("permissive parse should succeed for: %s", sql)
			}
			if stmt == nil {
				t.Fatalf("permissive parse returned nil for: %s", sql)
			}
		})
	}
}
