package integration

import (
	"context"
	"testing"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestIndexAdvisorRecommendsMissingIndex(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, name TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Run several queries that filter on email but have no index
	for i := 0; i < 5; i++ {
		_, err = db.Query(ctx, `SELECT * FROM users WHERE email = ?`, "test@example.com")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
	}

	recs := db.GetIndexRecommendations()
	if len(recs) == 0 {
		t.Fatal("expected index recommendations")
	}

	found := false
	for _, r := range recs {
		if r.TableName == "users" && len(r.Columns) == 1 && r.Columns[0] == "email" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected recommendation for users.email, got %+v", recs)
	}
}

func TestIndexAdvisorSkipsExistingIndex(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE products (id INTEGER PRIMARY KEY, sku TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	_, err = db.Exec(ctx, `CREATE INDEX idx_sku ON products(sku)`)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	for i := 0; i < 5; i++ {
		_, err = db.Query(ctx, `SELECT * FROM products WHERE sku = ?`, "ABC")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
	}

	recs := db.GetIndexRecommendations()
	for _, r := range recs {
		if r.TableName == "products" && len(r.Columns) == 1 && r.Columns[0] == "sku" {
			t.Error("should not recommend existing index")
		}
	}
}

func TestIndexAdvisorComposite(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, status TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	for i := 0; i < 5; i++ {
		_, err = db.Query(ctx, `SELECT * FROM orders WHERE user_id = ? AND status = ?`, 1, "pending")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
	}

	recs := db.GetIndexRecommendations()
	var composite bool
	for _, r := range recs {
		if r.TableName == "orders" && len(r.Columns) > 1 {
			composite = true
			break
		}
	}
	if !composite {
		t.Errorf("expected composite index recommendation, got %+v", recs)
	}
}

func TestIndexAdvisorResets(t *testing.T) {
	ctx := context.Background()
	db, err := engine.Open(":memory:", engine.DefaultOptions())
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, `CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	_, err = db.Query(ctx, `SELECT * FROM logs WHERE level = ?`, "error")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(db.GetIndexRecommendations()) == 0 {
		t.Fatal("expected recommendations before reset")
	}

	db.ResetIndexAdvisor()
	if len(db.GetIndexRecommendations()) != 0 {
		t.Error("expected no recommendations after reset")
	}
}
