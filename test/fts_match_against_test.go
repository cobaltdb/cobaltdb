package test

import (
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestFTS_MatchAgainst_Basic(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	// Create table with documents
	afExec(t, db, ctx, "CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT, content TEXT)")
	afExec(t, db, ctx, "INSERT INTO docs VALUES (1, 'Hello World', 'This is a hello world document')")
	afExec(t, db, ctx, "INSERT INTO docs VALUES (2, 'Go Programming', 'Go is a compiled programming language')")
	afExec(t, db, ctx, "INSERT INTO docs VALUES (3, 'SQL Tutorial', 'SQL is used for database queries')")
	afExec(t, db, ctx, "INSERT INTO docs VALUES (4, 'Hello Go', 'Learning Go programming with examples')")

	// Create full-text index
	afExec(t, db, ctx, "CREATE FULLTEXT INDEX idx_docs ON docs(title, content)")

	t.Run("MATCH AGAINST single word", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM docs WHERE MATCH(title, content) AGAINST('hello')")
		// Should match docs 1 (Hello World) and 4 (Hello Go)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("MATCH AGAINST different word", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM docs WHERE MATCH(title, content) AGAINST('programming')")
		// Should match docs 2 (Go Programming) and 4 (Go programming)
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("MATCH AGAINST no match", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT * FROM docs WHERE MATCH(title, content) AGAINST('nonexistent')")
		if len(rows) != 0 {
			t.Errorf("Expected 0 rows, got %d", len(rows))
		}
	})

	t.Run("MATCH AGAINST with SELECT columns", func(t *testing.T) {
		rows := afQuery(t, db, ctx, "SELECT id, title FROM docs WHERE MATCH(title, content) AGAINST('sql')")
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
		if len(rows) > 0 && rows[0][0] != int64(3) {
			t.Errorf("Expected id=3, got %v", rows[0][0])
		}
	})
}

func TestFTS_MatchAgainst_MultipleWords(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT)")
	afExec(t, db, ctx, "INSERT INTO articles VALUES (1, 'Go Tutorial', 'Learn Go programming step by step')")
	afExec(t, db, ctx, "INSERT INTO articles VALUES (2, 'Python Guide', 'Python programming for beginners')")
	afExec(t, db, ctx, "INSERT INTO articles VALUES (3, 'Go vs Python', 'Comparing Go and Python programming languages')")

	afExec(t, db, ctx, "CREATE FULLTEXT INDEX idx_articles ON articles(title, body)")

	t.Run("MATCH AGAINST multiple words AND logic", func(t *testing.T) {
		// Search for "Go programming" - should find docs containing both words
		rows := afQuery(t, db, ctx, "SELECT * FROM articles WHERE MATCH(title, body) AGAINST('Go programming')")
		// Doc 1 has 'Go' and 'programming', Doc 2 has 'programming' but not 'Go', Doc 3 has both
		// The FTS uses AND logic for multiple words
		if len(rows) != 2 {
			t.Errorf("Expected 2 rows, got %d", len(rows))
		}
	})
}

func TestFTS_MatchAgainst_SingleColumn(t *testing.T) {
	db, err := engine.Open(":memory:", &engine.Options{InMemory: true})
	if err != nil {
		t.Fatalf("DB open: %v", err)
	}
	defer db.Close()
	ctx := t.Context()

	afExec(t, db, ctx, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, description TEXT)")
	afExec(t, db, ctx, "INSERT INTO products VALUES (1, 'Apple iPhone', 'A smartphone made by Apple')")
	afExec(t, db, ctx, "INSERT INTO products VALUES (2, 'Apple Juice', 'Fresh apple juice drink')")
	afExec(t, db, ctx, "INSERT INTO products VALUES (3, 'Banana Phone', 'A phone shaped like a banana')")

	afExec(t, db, ctx, "CREATE FULLTEXT INDEX idx_products ON products(name, description)")

	t.Run("MATCH AGAINST search in name only effectively", func(t *testing.T) {
		// Search for iPhone - should find only the first product
		rows := afQuery(t, db, ctx, "SELECT * FROM products WHERE MATCH(name, description) AGAINST('iPhone')")
		if len(rows) != 1 {
			t.Errorf("Expected 1 row, got %d", len(rows))
		}
	})
}
