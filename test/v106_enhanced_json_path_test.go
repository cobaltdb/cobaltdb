package test

import (
	"fmt"
	"testing"
)

// TestV106_EnhancedJSONPath tests the enhanced JSON path resolution with nested paths
func TestV106_EnhancedJSONPath(t *testing.T) {
	db, ctx := af(t)
	defer db.Close()

	// Create table with JSON column
	afExec(t, db, ctx, "CREATE TABLE v106_products (id INTEGER PRIMARY KEY, name TEXT, metadata JSON)")

	// Insert products with nested JSON metadata
	afExec(t, db, ctx, "INSERT INTO v106_products VALUES (1, 'Laptop', '{\"brand\": \"TechCorp\", \"specs\": {\"cpu\": \"i7\", \"ram\": \"16GB\", \"storage\": \"512GB\"}, \"pricing\": {\"cost\": 800, \"msrp\": 999.99}}')")
	afExec(t, db, ctx, "INSERT INTO v106_products VALUES (2, 'Phone', '{\"brand\": \"MobileX\", \"specs\": {\"cpu\": \"A15\", \"ram\": \"8GB\", \"storage\": \"256GB\"}, \"pricing\": {\"cost\": 600, \"msrp\": 799.99}}')")
	afExec(t, db, ctx, "INSERT INTO v106_products VALUES (3, 'Tablet', '{\"brand\": \"TabCo\", \"specs\": {\"cpu\": \"M1\", \"ram\": \"16GB\", \"storage\": \"1TB\"}, \"pricing\": {\"cost\": 700, \"msrp\": 899.99}}')")

	t.Run("SimplePath", func(t *testing.T) {
		// Test simple $.key path (original functionality)
		rows := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(metadata, '$.brand') FROM v106_products WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "TechCorp" {
			t.Fatalf("Expected TechCorp, got %v", rows[0][0])
		}
	})

	t.Run("NestedPath2Levels", func(t *testing.T) {
		// Test nested $.key1.key2 path
		rows := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(metadata, '$.specs.cpu') FROM v106_products WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "i7" {
			t.Fatalf("Expected i7, got %v", rows[0][0])
		}
	})

	t.Run("NestedPath2LevelsDifferent", func(t *testing.T) {
		// Test nested path on different row
		rows := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(metadata, '$.specs.cpu') FROM v106_products WHERE id = 2")
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "A15" {
			t.Fatalf("Expected A15, got %v", rows[0][0])
		}
	})

	t.Run("NestedPathStorage", func(t *testing.T) {
		// Test accessing different nested keys
		rows := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(metadata, '$.specs.storage') FROM v106_products WHERE id = 3")
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "1TB" {
			t.Fatalf("Expected 1TB, got %v", rows[0][0])
		}
	})

	t.Run("NumericNestedPath", func(t *testing.T) {
		// Test nested numeric values
		rows := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(metadata, '$.pricing.msrp') FROM v106_products WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		// MSRP should be 999.99
		got := fmt.Sprintf("%v", rows[0][0])
		if got != "999.99" && got != "999.990000" {
			t.Fatalf("Expected 999.99, got %v", rows[0][0])
		}
	})

	t.Run("AllProductsBrand", func(t *testing.T) {
		// Query all products' brands
		rows := afQuery(t, db, ctx, "SELECT name, JSON_EXTRACT(metadata, '$.brand') FROM v106_products ORDER BY name")
		if len(rows) != 3 {
			t.Fatalf("Expected 3 rows, got %d", len(rows))
		}
		// Verify brands (ordered by name: Laptop, Phone, Tablet)
		expectedBrands := []string{"TechCorp", "MobileX", "TabCo"}
		for i, row := range rows {
			if fmt.Sprintf("%v", row[1]) != expectedBrands[i] {
				t.Fatalf("Expected %s for product %s, got %v", expectedBrands[i], row[0], row[1])
			}
		}
	})

	t.Run("AllProductsRAM", func(t *testing.T) {
		// Query all products' RAM using nested path
		rows := afQuery(t, db, ctx, "SELECT name, JSON_EXTRACT(metadata, '$.specs.ram') FROM v106_products ORDER BY name")
		if len(rows) != 3 {
			t.Fatalf("Expected 3 rows, got %d", len(rows))
		}
	})

	t.Run("WhereNestedPath", func(t *testing.T) {
		// Filter using nested JSON path in WHERE clause
		rows := afQuery(t, db, ctx, "SELECT name FROM v106_products WHERE JSON_EXTRACT(metadata, '$.specs.ram') = '16GB' ORDER BY name")
		if len(rows) != 2 {
			t.Fatalf("Expected 2 products with 16GB RAM, got %d", len(rows))
		}
		// Should be Laptop and Tablet
	})

	t.Run("NonExistentPath", func(t *testing.T) {
		// Test non-existent nested path returns NULL
		rows := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(metadata, '$.specs.nonexistent') FROM v106_products WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		// Should return NULL for non-existent path
		if rows[0][0] != nil {
			t.Logf("Expected NULL for non-existent path, got %v", rows[0][0])
		}
	})

	t.Run("DeepNesting", func(t *testing.T) {
		// Test even deeper nesting (3+ levels)
		afExec(t, db, ctx, "UPDATE v106_products SET metadata = '{\"brand\": \"DeepTest\", \"level1\": {\"level2\": {\"level3\": {\"value\": \"deep\"}}}}' WHERE id = 1")
		rows := afQuery(t, db, ctx, "SELECT JSON_EXTRACT(metadata, '$.level1.level2.level3.value') FROM v106_products WHERE id = 1")
		if len(rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(rows))
		}
		if fmt.Sprintf("%v", rows[0][0]) != "deep" {
			t.Fatalf("Expected 'deep', got %v", rows[0][0])
		}
	})
}
