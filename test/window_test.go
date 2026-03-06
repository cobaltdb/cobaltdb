package test

import (
	"fmt"
	"testing"
)

func TestWindowFunctions(t *testing.T) {
	db, ctx := af(t)

	// Setup test data
	afExec(t, db, ctx, "CREATE TABLE emp (id INTEGER PRIMARY KEY, name TEXT, dept TEXT, salary REAL)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (1, 'Alice', 'Eng', 120000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (2, 'Bob', 'Eng', 100000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (3, 'Carol', 'Mkt', 90000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (4, 'Dave', 'Eng', 110000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (5, 'Eve', 'Mkt', 85000)")
	afExec(t, db, ctx, "INSERT INTO emp VALUES (6, 'Frank', 'Sales', 95000)")

	// Test 1: ROW_NUMBER() OVER (ORDER BY salary DESC)
	rows := afQuery(t, db, ctx, "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM emp")
	t.Logf("ROW_NUMBER: %v", rows)
	if len(rows) != 6 {
		t.Fatalf("ROW_NUMBER: expected 6 rows, got %d", len(rows))
	}
	// First row (highest salary = Alice) should have rn=1
	for _, row := range rows {
		if fmt.Sprintf("%v", row[0]) == "Alice" {
			if fmt.Sprintf("%v", row[2]) != "1" {
				t.Errorf("ROW_NUMBER: Alice should be 1, got %v", row[2])
			}
		}
	}

	// Test 2: ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)
	rows = afQuery(t, db, ctx, "SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM emp")
	t.Logf("ROW_NUMBER PARTITION: %v", rows)
	if len(rows) != 6 {
		t.Fatalf("ROW_NUMBER PARTITION: expected 6 rows, got %d", len(rows))
	}
	// Alice should be rn=1 in Eng, Bob should be rn=3 in Eng, Carol should be rn=1 in Mkt
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		rn := fmt.Sprintf("%v", row[3])
		switch name {
		case "Alice":
			if rn != "1" {
				t.Errorf("ROW_NUMBER PARTITION: Alice should be 1 in Eng, got %v", rn)
			}
		case "Dave":
			if rn != "2" {
				t.Errorf("ROW_NUMBER PARTITION: Dave should be 2 in Eng, got %v", rn)
			}
		case "Bob":
			if rn != "3" {
				t.Errorf("ROW_NUMBER PARTITION: Bob should be 3 in Eng, got %v", rn)
			}
		case "Carol":
			if rn != "1" {
				t.Errorf("ROW_NUMBER PARTITION: Carol should be 1 in Mkt, got %v", rn)
			}
		}
	}

	// Test 3: RANK() with ties
	afExec(t, db, ctx, "CREATE TABLE scores (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (1, 'A', 100)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (2, 'B', 90)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (3, 'C', 90)")
	afExec(t, db, ctx, "INSERT INTO scores VALUES (4, 'D', 80)")

	rows = afQuery(t, db, ctx, "SELECT name, score, RANK() OVER (ORDER BY score DESC) as rnk FROM scores")
	t.Logf("RANK: %v", rows)
	// A=1, B=2, C=2, D=4
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		rnk := fmt.Sprintf("%v", row[2])
		switch name {
		case "A":
			if rnk != "1" {
				t.Errorf("RANK: A should be 1, got %v", rnk)
			}
		case "B":
			if rnk != "2" {
				t.Errorf("RANK: B should be 2, got %v", rnk)
			}
		case "C":
			if rnk != "2" {
				t.Errorf("RANK: C should be 2, got %v", rnk)
			}
		case "D":
			if rnk != "4" {
				t.Errorf("RANK: D should be 4, got %v", rnk)
			}
		}
	}

	// Test 4: DENSE_RANK()
	rows = afQuery(t, db, ctx, "SELECT name, score, DENSE_RANK() OVER (ORDER BY score DESC) as drnk FROM scores")
	t.Logf("DENSE_RANK: %v", rows)
	// A=1, B=2, C=2, D=3
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		drnk := fmt.Sprintf("%v", row[2])
		switch name {
		case "A":
			if drnk != "1" {
				t.Errorf("DENSE_RANK: A should be 1, got %v", drnk)
			}
		case "D":
			if drnk != "3" {
				t.Errorf("DENSE_RANK: D should be 3, got %v", drnk)
			}
		}
	}

	// Test 5: LAG()
	rows = afQuery(t, db, ctx, "SELECT name, score, LAG(score) OVER (ORDER BY score DESC) as prev_score FROM scores")
	t.Logf("LAG: %v", rows)
	// A: prev=nil, B: prev=100, C: prev=90, D: prev=90
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		prev := fmt.Sprintf("%v", row[2])
		switch name {
		case "A":
			if prev != "<nil>" {
				t.Errorf("LAG: A should have nil prev, got %v", prev)
			}
		case "D":
			if prev != "90" {
				t.Errorf("LAG: D should have prev=90, got %v", prev)
			}
		}
	}

	// Test 6: LEAD()
	rows = afQuery(t, db, ctx, "SELECT name, score, LEAD(score) OVER (ORDER BY score DESC) as next_score FROM scores")
	t.Logf("LEAD: %v", rows)
	// A: next=90, B: next=90, C: next=80, D: next=nil
	for _, row := range rows {
		name := fmt.Sprintf("%v", row[0])
		next := fmt.Sprintf("%v", row[2])
		switch name {
		case "A":
			if next != "90" {
				t.Errorf("LEAD: A should have next=90, got %v", next)
			}
		case "D":
			if next != "<nil>" {
				t.Errorf("LEAD: D should have nil next, got %v", next)
			}
		}
	}

	t.Log("All window function tests passed!")
}
