package engine

import (
	"context"
	"strings"
	"testing"
)

func TestStrictSQLParsingRejectsTrailingTokens(t *testing.T) {
	db, err := Open(":memory:", &Options{
		CoreStorage: CoreStorage{InMemory: true},
		Security:    Security{StrictSQLParsing: true},
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	_, err = db.Query(context.Background(), "SELECT * FROM users TABLESAMPLE SYSTEM (10)")
	if err == nil {
		t.Fatal("expected strict parse error")
	}
	if !strings.Contains(err.Error(), "unexpected token after statement") {
		t.Fatalf("expected strict trailing token error, got %v", err)
	}
}

func TestDefaultSQLParsingRemainsCompatible(t *testing.T) {
	// The default parser now also rejects trailing tokens: previously
	// `... TABLESAMPLE SYSTEM (10)` was silently truncated and the wrong
	// statement executed. Only strict mode adds token-sequence strictness on
	// top; statement-end validation applies to both. Legitimate statements
	// (including trailing semicolons) must keep parsing.
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	_, err = db.Query(context.Background(), "SELECT * FROM users TABLESAMPLE SYSTEM (10)")
	if err == nil {
		t.Fatal("expected parse error for trailing tokens")
	}
	if !strings.Contains(err.Error(), "unexpected token after statement") {
		t.Fatalf("expected trailing token error, got %v", err)
	}

	if _, err := db.Exec(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY);"); err != nil {
		t.Fatalf("statement with trailing semicolon must parse: %v", err)
	}
}
