package test

import (
	"fmt"
	"testing"
)

// TestV50SchemaOps exercises schema modification operations: ALTER TABLE,
// CREATE/DROP INDEX, CREATE/DROP VIEW, triggers, and DDL+DML interactions.
func TestV50SchemaOps(t *testing.T) {
	db, ctx := af(t)
	pass := 0
	total := 0

	check := func(desc string, sql string, expected interface{}) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) == 0 || len(rows[0]) == 0 {
			t.Errorf("[FAIL] %s: no rows returned", desc)
			return
		}
		got := fmt.Sprintf("%v", rows[0][0])
		exp := fmt.Sprintf("%v", expected)
		if got != exp {
			t.Errorf("[FAIL] %s: got %s, expected %s", desc, got, exp)
			return
		}
		pass++
	}

	checkRowCount := func(desc string, sql string, expected int) {
		t.Helper()
		total++
		rows := afQuery(t, db, ctx, sql)
		if len(rows) != expected {
			t.Errorf("[FAIL] %s: expected %d rows, got %d", desc, expected, len(rows))
			return
		}
		pass++
	}

	checkNoError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err != nil {
			t.Errorf("[FAIL] %s: %v", desc, err)
			return
		}
		pass++
	}

	checkError := func(desc string, sql string) {
		t.Helper()
		total++
		_, err := db.Exec(ctx, sql)
		if err == nil {
			t.Errorf("[FAIL] %s: expected error but got nil", desc)
			return
		}
		pass++
	}

	// ============================================================
	// === ALTER TABLE ADD COLUMN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v50_items (id INTEGER PRIMARY KEY, name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v50_items VALUES (1, 'Widget')")
	afExec(t, db, ctx, "INSERT INTO v50_items VALUES (2, 'Gadget')")

	// AC1: Add a new column
	checkNoError("AC1 ADD COLUMN", "ALTER TABLE v50_items ADD COLUMN price INTEGER")

	// AC2: Existing rows have NULL for new column
	check("AC2 new col is NULL",
		"SELECT COALESCE(price, 0) FROM v50_items WHERE id = 1", 0)

	// AC3: Can insert with new column
	checkNoError("AC3 insert with new col",
		"INSERT INTO v50_items VALUES (3, 'Doohickey', 25)")

	check("AC3 verify", "SELECT price FROM v50_items WHERE id = 3", 25)

	// AC4: Can update new column
	checkNoError("AC4 update new col",
		"UPDATE v50_items SET price = 10 WHERE id = 1")
	check("AC4 verify", "SELECT price FROM v50_items WHERE id = 1", 10)

	// AC5: Add column with DEFAULT
	checkNoError("AC5 ADD COLUMN DEFAULT",
		"ALTER TABLE v50_items ADD COLUMN quantity INTEGER DEFAULT 0")

	// AC6: Duplicate column name should error
	checkError("AC6 duplicate column", "ALTER TABLE v50_items ADD COLUMN name TEXT")

	// ============================================================
	// === ALTER TABLE DROP COLUMN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v50_drop_test (
		id INTEGER PRIMARY KEY, a TEXT, b INTEGER, c REAL)`)
	afExec(t, db, ctx, "INSERT INTO v50_drop_test VALUES (1, 'hello', 42, 3.14)")

	// DC1: Drop a column
	checkNoError("DC1 DROP COLUMN", "ALTER TABLE v50_drop_test DROP COLUMN c")

	// DC2: Dropped column is gone
	checkError("DC2 select dropped col", "SELECT c FROM v50_drop_test")

	// DC3: Other columns still work
	check("DC3 remaining cols", "SELECT b FROM v50_drop_test WHERE id = 1", 42)

	// DC4: Cannot drop PRIMARY KEY
	checkError("DC4 drop PK", "ALTER TABLE v50_drop_test DROP COLUMN id")

	// ============================================================
	// === ALTER TABLE RENAME COLUMN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v50_rename (id INTEGER PRIMARY KEY, old_name TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v50_rename VALUES (1, 'test')")

	// RC1: Rename column
	checkNoError("RC1 RENAME COLUMN",
		"ALTER TABLE v50_rename RENAME COLUMN old_name TO new_name")

	// RC2: Old name no longer works
	checkError("RC2 old name fails", "SELECT old_name FROM v50_rename")

	// RC3: New name works
	check("RC3 new name works", "SELECT new_name FROM v50_rename WHERE id = 1", "test")

	// ============================================================
	// === CREATE/DROP INDEX ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v50_indexed (
		id INTEGER PRIMARY KEY, name TEXT, category TEXT, price INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v50_indexed VALUES (1, 'A', 'cat1', 100)")
	afExec(t, db, ctx, "INSERT INTO v50_indexed VALUES (2, 'B', 'cat2', 200)")
	afExec(t, db, ctx, "INSERT INTO v50_indexed VALUES (3, 'C', 'cat1', 150)")
	afExec(t, db, ctx, "INSERT INTO v50_indexed VALUES (4, 'D', 'cat2', 250)")
	afExec(t, db, ctx, "INSERT INTO v50_indexed VALUES (5, 'E', 'cat1', 175)")

	// IX1: Create index
	checkNoError("IX1 CREATE INDEX", "CREATE INDEX idx_category ON v50_indexed(category)")

	// IX2: Query still works with index
	check("IX2 query with index",
		"SELECT COUNT(*) FROM v50_indexed WHERE category = 'cat1'", 3)

	// IX3: Create UNIQUE index
	checkNoError("IX3 CREATE UNIQUE INDEX",
		"CREATE UNIQUE INDEX idx_name ON v50_indexed(name)")

	// IX4: UNIQUE index prevents duplicates
	checkError("IX4 unique violation",
		"INSERT INTO v50_indexed VALUES (6, 'A', 'cat3', 300)")

	// IX5: Drop index
	checkNoError("IX5 DROP INDEX", "DROP INDEX idx_category")

	// IX6: Queries still work after drop
	check("IX6 query after drop",
		"SELECT COUNT(*) FROM v50_indexed WHERE category = 'cat1'", 3)

	// ============================================================
	// === CREATE/DROP VIEW ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v50_products (
		id INTEGER PRIMARY KEY, name TEXT, price INTEGER, active INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v50_products VALUES (1, 'Phone', 999, 1)")
	afExec(t, db, ctx, "INSERT INTO v50_products VALUES (2, 'Tablet', 499, 1)")
	afExec(t, db, ctx, "INSERT INTO v50_products VALUES (3, 'Watch', 299, 0)")
	afExec(t, db, ctx, "INSERT INTO v50_products VALUES (4, 'Laptop', 1499, 1)")

	// VW1: Create view
	checkNoError("VW1 CREATE VIEW",
		"CREATE VIEW v50_active_products AS SELECT * FROM v50_products WHERE active = 1")

	// VW2: Query view
	check("VW2 query view",
		"SELECT COUNT(*) FROM v50_active_products", 3)

	// VW3: View reflects underlying data
	afExec(t, db, ctx, "UPDATE v50_products SET active = 1 WHERE id = 3")
	check("VW3 view updated", "SELECT COUNT(*) FROM v50_active_products", 4)

	// VW4: View with JOIN
	afExec(t, db, ctx, `CREATE TABLE v50_reviews (
		product_id INTEGER, rating INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v50_reviews VALUES (1, 5)")
	afExec(t, db, ctx, "INSERT INTO v50_reviews VALUES (1, 4)")
	afExec(t, db, ctx, "INSERT INTO v50_reviews VALUES (2, 3)")

	checkNoError("VW4 CREATE VIEW with aggregate",
		`CREATE VIEW v50_avg_ratings AS
		 SELECT product_id, AVG(rating) AS avg_rating FROM v50_reviews GROUP BY product_id`)

	check("VW4 query aggregate view",
		"SELECT avg_rating FROM v50_avg_ratings WHERE product_id = 1", 4.5)

	// VW5: DROP VIEW
	checkNoError("VW5 DROP VIEW", "DROP VIEW v50_active_products")
	checkError("VW5 view gone", "SELECT * FROM v50_active_products")

	// ============================================================
	// === TRIGGERS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v50_orders (
		id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER, total INTEGER)`)
	afExec(t, db, ctx, `CREATE TABLE v50_audit_log (
		action TEXT, order_id INTEGER, ts TEXT DEFAULT 'now')`)

	// TR1: INSERT trigger
	checkNoError("TR1 CREATE TRIGGER",
		`CREATE TRIGGER v50_after_insert AFTER INSERT ON v50_orders
		 BEGIN
		   INSERT INTO v50_audit_log (action, order_id) VALUES ('INSERT', NEW.id);
		 END`)

	checkNoError("TR1 trigger fires",
		"INSERT INTO v50_orders VALUES (1, 'Widget', 5, 50)")

	check("TR1 audit logged",
		"SELECT action FROM v50_audit_log WHERE order_id = 1", "INSERT")

	// TR2: Multiple inserts trigger multiple logs
	checkNoError("TR2 second insert",
		"INSERT INTO v50_orders VALUES (2, 'Gadget', 3, 30)")

	check("TR2 audit count", "SELECT COUNT(*) FROM v50_audit_log", 2)

	// TR3: UPDATE trigger
	checkNoError("TR3 CREATE UPDATE TRIGGER",
		`CREATE TRIGGER v50_after_update AFTER UPDATE ON v50_orders
		 BEGIN
		   INSERT INTO v50_audit_log (action, order_id) VALUES ('UPDATE', NEW.id);
		 END`)

	checkNoError("TR3 update fires trigger",
		"UPDATE v50_orders SET total = 55 WHERE id = 1")

	check("TR3 update logged",
		"SELECT COUNT(*) FROM v50_audit_log WHERE action = 'UPDATE'", 1)

	// TR4: DELETE trigger
	checkNoError("TR4 CREATE DELETE TRIGGER",
		`CREATE TRIGGER v50_after_delete AFTER DELETE ON v50_orders
		 BEGIN
		   INSERT INTO v50_audit_log (action, order_id) VALUES ('DELETE', OLD.id);
		 END`)

	checkNoError("TR4 delete fires trigger",
		"DELETE FROM v50_orders WHERE id = 2")

	check("TR4 delete logged",
		"SELECT COUNT(*) FROM v50_audit_log WHERE action = 'DELETE'", 1)

	// TR5: DROP TRIGGER
	checkNoError("TR5 DROP TRIGGER", "DROP TRIGGER v50_after_insert")

	// TR6: Trigger no longer fires
	checkNoError("TR6 insert after drop",
		"INSERT INTO v50_orders VALUES (3, 'Thing', 1, 10)")

	check("TR6 no new INSERT audit",
		"SELECT COUNT(*) FROM v50_audit_log WHERE action = 'INSERT'", 2) // Still 2 from before

	// ============================================================
	// === DDL + DML INTERACTIONS ===
	// ============================================================

	// DI1: CREATE TABLE IF NOT EXISTS
	checkNoError("DI1 IF NOT EXISTS",
		"CREATE TABLE IF NOT EXISTS v50_items (id INTEGER PRIMARY KEY, dummy TEXT)")

	// DI2: Existing table not affected
	check("DI2 table unchanged",
		"SELECT name FROM v50_items WHERE id = 1", "Widget")

	// DI3: DROP TABLE IF EXISTS
	afExec(t, db, ctx, "CREATE TABLE v50_temp (id INTEGER PRIMARY KEY)")
	checkNoError("DI3 DROP IF EXISTS", "DROP TABLE IF EXISTS v50_temp")
	checkNoError("DI3 DROP nonexistent", "DROP TABLE IF EXISTS v50_nonexistent")

	// DI4: Insert after ALTER TABLE
	checkNoError("DI4 insert after alter",
		"INSERT INTO v50_items (id, name, price, quantity) VALUES (4, 'Thingamajig', 50, 10)")

	check("DI4 verify all cols",
		"SELECT price FROM v50_items WHERE id = 4", 50)

	// DI5: SELECT after DROP TABLE
	afExec(t, db, ctx, "CREATE TABLE v50_disposable (id INTEGER PRIMARY KEY, val TEXT)")
	afExec(t, db, ctx, "INSERT INTO v50_disposable VALUES (1, 'test')")
	afExec(t, db, ctx, "DROP TABLE v50_disposable")
	checkError("DI5 select dropped table", "SELECT * FROM v50_disposable")

	// ============================================================
	// === MULTIPLE OPERATIONS IN TRANSACTION ===
	// ============================================================

	// MT1: DDL + DML in transaction
	checkNoError("MT1 BEGIN", "BEGIN")
	checkNoError("MT1 CREATE TABLE",
		"CREATE TABLE v50_txn_test (id INTEGER PRIMARY KEY, val TEXT)")
	checkNoError("MT1 INSERT",
		"INSERT INTO v50_txn_test VALUES (1, 'in-txn')")
	check("MT1 visible in txn",
		"SELECT val FROM v50_txn_test WHERE id = 1", "in-txn")
	checkNoError("MT1 COMMIT", "COMMIT")

	check("MT1 persisted",
		"SELECT val FROM v50_txn_test WHERE id = 1", "in-txn")

	// ============================================================
	// === INDEX USAGE WITH UPDATES ===
	// ============================================================

	// IU1: Index maintained after INSERT
	checkNoError("IU1 create index",
		"CREATE INDEX idx_price ON v50_indexed(price)")

	checkNoError("IU1 insert with index",
		"INSERT INTO v50_indexed VALUES (6, 'F', 'cat3', 50)")

	check("IU1 index query after insert",
		"SELECT name FROM v50_indexed WHERE price = 50", "F")

	// IU2: Index maintained after UPDATE
	checkNoError("IU2 update indexed col",
		"UPDATE v50_indexed SET price = 999 WHERE name = 'A'")

	check("IU2 old value gone",
		"SELECT COUNT(*) FROM v50_indexed WHERE price = 100", 0)

	// IU3: Index maintained after DELETE
	checkNoError("IU3 delete with index",
		"DELETE FROM v50_indexed WHERE name = 'F'")

	check("IU3 deleted row gone",
		"SELECT COUNT(*) FROM v50_indexed WHERE price = 50", 0)

	// ============================================================
	// === TABLE WITH MANY CONSTRAINTS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v50_constrained (
		id INTEGER PRIMARY KEY,
		email TEXT UNIQUE NOT NULL,
		age INTEGER CHECK(age >= 0 AND age <= 150),
		status TEXT DEFAULT 'active')`)

	// CN1: Valid insert
	checkNoError("CN1 valid insert",
		"INSERT INTO v50_constrained (id, email, age) VALUES (1, 'a@b.com', 25)")

	check("CN1 default applied",
		"SELECT status FROM v50_constrained WHERE id = 1", "active")

	// CN2: UNIQUE violation
	checkError("CN2 unique email", "INSERT INTO v50_constrained (id, email, age) VALUES (2, 'a@b.com', 30)")

	// CN3: NOT NULL violation
	checkError("CN3 NOT NULL email", "INSERT INTO v50_constrained (id, email, age) VALUES (2, NULL, 30)")

	// CN4: CHECK violation
	checkError("CN4 CHECK age", "INSERT INTO v50_constrained (id, email, age) VALUES (2, 'b@b.com', -1)")

	// CN5: All constraints pass
	checkNoError("CN5 all constraints ok",
		"INSERT INTO v50_constrained (id, email, age, status) VALUES (2, 'b@b.com', 30, 'inactive')")

	checkRowCount("CN5 verify rows", "SELECT * FROM v50_constrained", 2)

	t.Logf("\n=== V50 SCHEMA OPS: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
