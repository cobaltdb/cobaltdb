package test

import (
	"fmt"
	"testing"
)

func TestCompositeIndexUpdateBug(t *testing.T) {
	db, ctx := TestDB(t)

	// 1. Create table
	Exec(t, db, ctx, "CREATE TABLE t (id INTEGER PRIMARY KEY, cat TEXT, score INTEGER)")

	// 2. Create composite index
	Exec(t, db, ctx, "CREATE INDEX idx_cat_score ON t(cat, score)")

	// 3. Insert rows
	Exec(t, db, ctx, "INSERT INTO t VALUES (1, 'A', 10)")
	Exec(t, db, ctx, "INSERT INTO t VALUES (2, 'A', 20)")
	Exec(t, db, ctx, "INSERT INTO t VALUES (3, 'B', 10)")
	Exec(t, db, ctx, "INSERT INTO t VALUES (4, 'B', 20)")
	Exec(t, db, ctx, "INSERT INTO t VALUES (5, 'A', 30)")

	// Verify initial state
	t.Log("=== Initial state: SELECT id, score FROM t WHERE cat = 'A' ===")
	rows := Query(t, db, ctx, "SELECT id, score FROM t WHERE cat = 'A'")
	for rows.Next() {
		var id, score interface{}
		if err := rows.Scan(&id, &score); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("  id=%v score=%v", id, score)
	}
	rows.Close()

	// 4. Update: change score from 10 to 99 for cat='A'
	t.Log("=== Running: UPDATE t SET score = 99 WHERE cat = 'A' AND score = 10 ===")
	Exec(t, db, ctx, "UPDATE t SET score = 99 WHERE cat = 'A' AND score = 10")

	// 5. Query for updated rows: expect 1 row (id=1, score=99)
	t.Log("=== Test 1: SELECT id FROM t WHERE cat = 'A' AND score = 99 (expect 1 row: id=1) ===")
	rows = Query(t, db, ctx, "SELECT id FROM t WHERE cat = 'A' AND score = 99")
	count := 0
	for rows.Next() {
		var id interface{}
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("  id=%v", id)
		count++
	}
	rows.Close()
	if count != 1 {
		t.Errorf("FAIL Test 1: expected 1 row, got %d", count)
	} else {
		t.Log("  PASS Test 1")
	}

	// 6. Query for old value: expect 0 rows (score=10 no longer exists for cat='A')
	t.Log("=== Test 2: SELECT id FROM t WHERE cat = 'A' AND score = 10 (expect 0 rows) ===")
	rows = Query(t, db, ctx, "SELECT id FROM t WHERE cat = 'A' AND score = 10")
	count = 0
	for rows.Next() {
		var id interface{}
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("  id=%v (UNEXPECTED!)", id)
		count++
	}
	rows.Close()
	if count != 0 {
		t.Errorf("FAIL Test 2: expected 0 rows, got %d", count)
	} else {
		t.Log("  PASS Test 2")
	}

	// 7. Full scan of cat='A' rows to verify data integrity
	t.Log("=== Test 3: SELECT id, score FROM t WHERE cat = 'A' (expect 3 rows with correct scores) ===")
	rows = Query(t, db, ctx, "SELECT id, score FROM t WHERE cat = 'A'")
	count = 0
	expectedScores := map[float64]float64{1: 99, 2: 20, 5: 30} // id -> expected score
	for rows.Next() {
		var id, score interface{}
		if err := rows.Scan(&id, &score); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("  id=%v score=%v", id, score)
		count++

		idF, ok1 := id.(float64)
		scoreF, ok2 := score.(float64)
		if ok1 && ok2 {
			if exp, exists := expectedScores[idF]; exists && exp != scoreF {
				t.Errorf("  WRONG SCORE: id=%v expected score=%v got score=%v", id, exp, score)
			}
		}
	}
	rows.Close()
	if count != 3 {
		t.Errorf("FAIL Test 3: expected 3 rows, got %d", count)
	} else {
		t.Log("  PASS Test 3")
	}

	// 8. Cross-check: a full table scan without WHERE on cat
	t.Log("=== Test 4: SELECT id, cat, score FROM t ORDER BY id (full table dump) ===")
	rows = Query(t, db, ctx, "SELECT id, cat, score FROM t ORDER BY id")
	for rows.Next() {
		var id, cat, score interface{}
		if err := rows.Scan(&id, &cat, &score); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		t.Logf("  id=%v cat=%v score=%v", id, cat, score)
	}
	rows.Close()

	// 9. Count check with exact conditions matching the reported bug pattern
	t.Log("=== Test 5: COUNT(*) checks ===")
	rows = Query(t, db, ctx, "SELECT COUNT(*) FROM t WHERE cat = 'A' AND score = 99")
	if rows.Next() {
		var cnt interface{}
		rows.Scan(&cnt)
		t.Logf("  COUNT(*) WHERE cat='A' AND score=99: %v (expect 1)", cnt)
		if fmt.Sprintf("%v", cnt) != "1" {
			t.Errorf("FAIL Test 5a: expected count=1, got %v", cnt)
		}
	}
	rows.Close()

	rows = Query(t, db, ctx, "SELECT COUNT(*) FROM t WHERE cat = 'A'")
	if rows.Next() {
		var cnt interface{}
		rows.Scan(&cnt)
		t.Logf("  COUNT(*) WHERE cat='A': %v (expect 3)", cnt)
		if fmt.Sprintf("%v", cnt) != "3" {
			t.Errorf("FAIL Test 5b: expected count=3, got %v", cnt)
		}
	}
	rows.Close()
}
