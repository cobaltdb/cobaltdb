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
	db, err := Open(":memory:", &Options{CoreStorage: CoreStorage{InMemory: true}})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	_, err = db.Query(context.Background(), "SELECT * FROM users TABLESAMPLE SYSTEM (10)")
	if err == nil {
		t.Fatal("expected runtime table error")
	}
	if strings.Contains(err.Error(), "unexpected token after statement") {
		t.Fatalf("default parser should remain permissive, got %v", err)
	}
}
