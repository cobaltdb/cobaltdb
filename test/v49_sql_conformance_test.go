package test

import (
	"fmt"
	"testing"
)

// TestV49SQLConformance verifies SQL standard conformance across complex patterns:
// multi-table JOINs, LEFT/RIGHT JOIN edge cases, nested CTEs, window functions
// with PARTITION BY + ORDER BY on non-SELECT columns, NULL propagation, etc.
func TestV49SQLConformance(t *testing.T) {
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

	_ = checkNoError

	// ============================================================
	// Setup: School database
	// ============================================================
	afExec(t, db, ctx, `CREATE TABLE v49_teachers (
		id INTEGER PRIMARY KEY, name TEXT, subject TEXT, years_exp INTEGER)`)
	afExec(t, db, ctx, "INSERT INTO v49_teachers VALUES (1, 'Smith', 'Math', 15)")
	afExec(t, db, ctx, "INSERT INTO v49_teachers VALUES (2, 'Jones', 'Science', 10)")
	afExec(t, db, ctx, "INSERT INTO v49_teachers VALUES (3, 'Brown', 'Math', 8)")
	afExec(t, db, ctx, "INSERT INTO v49_teachers VALUES (4, 'Davis', 'English', 20)")
	afExec(t, db, ctx, "INSERT INTO v49_teachers VALUES (5, 'Wilson', 'Science', 5)")

	afExec(t, db, ctx, `CREATE TABLE v49_students (
		id INTEGER PRIMARY KEY, name TEXT, grade INTEGER, gpa REAL)`)
	afExec(t, db, ctx, "INSERT INTO v49_students VALUES (1, 'Amy', 10, 3.8)")
	afExec(t, db, ctx, "INSERT INTO v49_students VALUES (2, 'Ben', 11, 3.2)")
	afExec(t, db, ctx, "INSERT INTO v49_students VALUES (3, 'Cathy', 10, 3.9)")
	afExec(t, db, ctx, "INSERT INTO v49_students VALUES (4, 'Dan', 12, 2.8)")
	afExec(t, db, ctx, "INSERT INTO v49_students VALUES (5, 'Ella', 11, 3.5)")
	afExec(t, db, ctx, "INSERT INTO v49_students VALUES (6, 'Fred', 12, 3.1)")

	afExec(t, db, ctx, `CREATE TABLE v49_classes (
		id INTEGER PRIMARY KEY, teacher_id INTEGER, subject TEXT, period INTEGER,
		FOREIGN KEY (teacher_id) REFERENCES v49_teachers(id))`)
	afExec(t, db, ctx, "INSERT INTO v49_classes VALUES (1, 1, 'Algebra', 1)")
	afExec(t, db, ctx, "INSERT INTO v49_classes VALUES (2, 1, 'Calculus', 3)")
	afExec(t, db, ctx, "INSERT INTO v49_classes VALUES (3, 2, 'Physics', 2)")
	afExec(t, db, ctx, "INSERT INTO v49_classes VALUES (4, 3, 'Geometry', 4)")
	afExec(t, db, ctx, "INSERT INTO v49_classes VALUES (5, 4, 'Literature', 1)")
	afExec(t, db, ctx, "INSERT INTO v49_classes VALUES (6, 5, 'Biology', 2)")

	afExec(t, db, ctx, `CREATE TABLE v49_enrollments (
		student_id INTEGER, class_id INTEGER, score INTEGER,
		FOREIGN KEY (student_id) REFERENCES v49_students(id),
		FOREIGN KEY (class_id) REFERENCES v49_classes(id))`)
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (1, 1, 95)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (1, 3, 88)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (1, 5, 92)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (2, 2, 78)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (2, 6, 85)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (3, 1, 98)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (3, 4, 91)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (4, 2, 70)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (4, 5, 75)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (5, 3, 90)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (5, 4, 87)")
	afExec(t, db, ctx, "INSERT INTO v49_enrollments VALUES (6, 6, 82)")

	// ============================================================
	// === MULTI-TABLE JOINs (3+ tables) ===
	// ============================================================

	// MJ1: Three-table JOIN
	check("MJ1 three-table JOIN",
		`SELECT s.name FROM v49_students s
		 JOIN v49_enrollments e ON s.id = e.student_id
		 JOIN v49_classes c ON e.class_id = c.id
		 WHERE c.subject = 'Algebra'
		 ORDER BY e.score DESC LIMIT 1`,
		"Cathy") // Cathy:98 > Amy:95

	// MJ2: Four-table JOIN
	check("MJ2 four-table JOIN",
		`SELECT t.name FROM v49_teachers t
		 JOIN v49_classes c ON t.id = c.teacher_id
		 JOIN v49_enrollments e ON c.id = e.class_id
		 JOIN v49_students s ON e.student_id = s.id
		 WHERE s.name = 'Amy' AND c.subject = 'Algebra'`,
		"Smith")

	// MJ3: Three-table JOIN with aggregate
	check("MJ3 three-table aggregate",
		`SELECT t.name, AVG(e.score) AS avg_score
		 FROM v49_teachers t
		 JOIN v49_classes c ON t.id = c.teacher_id
		 JOIN v49_enrollments e ON c.id = e.class_id
		 GROUP BY t.name
		 ORDER BY avg_score DESC LIMIT 1`,
		"Jones") // Jones: (88+90)/2=89 > Smith: (95+98+78+70)/4=85.25

	// MJ4: Multi-table with DISTINCT
	check("MJ4 multi-table DISTINCT",
		`SELECT COUNT(DISTINCT s.id) FROM v49_students s
		 JOIN v49_enrollments e ON s.id = e.student_id
		 JOIN v49_classes c ON e.class_id = c.id
		 JOIN v49_teachers t ON c.teacher_id = t.id
		 WHERE t.subject = 'Math'`,
		5) // Amy(1), Ben(2), Cathy(3), Dan(4), Ella(5) via Math teachers' classes

	// ============================================================
	// === LEFT/RIGHT JOIN EDGE CASES ===
	// ============================================================

	// Create a teacher with no classes
	afExec(t, db, ctx, "INSERT INTO v49_teachers VALUES (6, 'Taylor', 'Art', 3)")

	// LJ1: LEFT JOIN - include unmatched
	checkRowCount("LJ1 LEFT JOIN unmatched",
		`SELECT t.name FROM v49_teachers t
		 LEFT JOIN v49_classes c ON t.id = c.teacher_id`,
		7) // 6 classes + Taylor(no classes)

	// LJ2: LEFT JOIN find unmatched (NULL check)
	check("LJ2 LEFT JOIN find NULL",
		`SELECT t.name FROM v49_teachers t
		 LEFT JOIN v49_classes c ON t.id = c.teacher_id
		 WHERE c.id IS NULL`,
		"Taylor")

	// LJ3: LEFT JOIN with aggregate
	check("LJ3 LEFT JOIN aggregate",
		`SELECT t.name, COUNT(c.id) AS class_count
		 FROM v49_teachers t
		 LEFT JOIN v49_classes c ON t.id = c.teacher_id
		 GROUP BY t.name
		 ORDER BY class_count ASC LIMIT 1`,
		"Taylor") // 0 classes

	// LJ4: LEFT JOIN preserves left rows through chain
	check("LJ4 LEFT JOIN chain",
		`SELECT COUNT(DISTINCT t.id) FROM v49_teachers t
		 LEFT JOIN v49_classes c ON t.id = c.teacher_id
		 LEFT JOIN v49_enrollments e ON c.id = e.class_id`,
		6) // All 6 teachers

	// ============================================================
	// === WINDOW FUNCTIONS ADVANCED ===
	// ============================================================

	// WA1: ROW_NUMBER PARTITION BY + ORDER BY on non-SELECT column
	check("WA1 ROW_NUMBER partition non-select",
		`SELECT student_name FROM (
		   SELECT s.name AS student_name, s.grade,
		          ROW_NUMBER() OVER (PARTITION BY s.grade ORDER BY s.gpa DESC) AS rn
		   FROM v49_students s
		 ) AS ranked WHERE rn = 1 AND grade = 10`,
		"Cathy") // Grade 10: Cathy(3.9) > Amy(3.8)

	// WA2: RANK with PARTITION BY
	check("WA2 RANK partition",
		`SELECT name FROM (
		   SELECT s.name, e.score,
		          RANK() OVER (PARTITION BY c.teacher_id ORDER BY e.score DESC) AS rk
		   FROM v49_students s
		   JOIN v49_enrollments e ON s.id = e.student_id
		   JOIN v49_classes c ON e.class_id = c.id
		   WHERE c.teacher_id = 1
		 ) AS ranked WHERE rk = 1`,
		"Cathy") // Smith's classes: Cathy(98), Amy(95), Ben(78), Dan(70)

	// WA3: SUM OVER entire partition (no frame clause = full partition sum)
	check("WA3 SUM OVER total",
		`SELECT total FROM (
		   SELECT score, SUM(score) OVER () AS total,
		          ROW_NUMBER() OVER (ORDER BY score) AS rn
		   FROM v49_enrollments
		 ) AS running WHERE rn = 1`,
		1031) // Sum of all scores

	// WA4: Window function with expression in ORDER BY
	check("WA4 window expr ORDER BY",
		`SELECT name FROM (
		   SELECT name, ROW_NUMBER() OVER (ORDER BY gpa * grade DESC) AS rn
		   FROM v49_students
		 ) AS ranked WHERE rn = 1`,
		"Cathy") // Cathy: 3.9*10=39, Amy: 3.8*10=38, Ella: 3.5*11=38.5, Fred: 3.1*12=37.2, Ben: 3.2*11=35.2, Dan: 2.8*12=33.6

	// ============================================================
	// === NESTED CTEs ===
	// ============================================================

	// NC1: CTE referencing previous CTE
	check("NC1 CTE chain",
		`WITH teacher_load AS (
		   SELECT t.id, t.name, COUNT(c.id) AS class_count
		   FROM v49_teachers t
		   LEFT JOIN v49_classes c ON t.id = c.teacher_id
		   GROUP BY t.id, t.name
		 ),
		 heavy_load AS (
		   SELECT * FROM teacher_load WHERE class_count >= 2
		 )
		 SELECT name FROM heavy_load ORDER BY class_count DESC LIMIT 1`,
		"Smith") // Smith has 2 classes (Algebra, Calculus)

	// NC2: CTE with UNION referencing table data
	check("NC2 CTE UNION",
		`WITH all_subjects AS (
		   SELECT DISTINCT subject FROM v49_teachers
		   UNION
		   SELECT DISTINCT subject FROM v49_classes
		 )
		 SELECT COUNT(*) FROM all_subjects`,
		10) // Math, Science, English, Art (4 teacher subjects) + 6 class subjects (no overlap) = 10

	// NC3: Multiple CTEs with JOIN between them
	check("NC3 multi-CTE JOIN",
		`WITH top_students AS (
		   SELECT id, name FROM v49_students WHERE gpa >= 3.5
		 ),
		 math_classes AS (
		   SELECT c.id FROM v49_classes c
		   JOIN v49_teachers t ON c.teacher_id = t.id
		   WHERE t.subject = 'Math'
		 )
		 SELECT COUNT(*) FROM top_students ts
		 JOIN v49_enrollments e ON ts.id = e.student_id
		 JOIN math_classes mc ON e.class_id = mc.id`,
		4) // Amy(Algebra), Cathy(Algebra,Geometry), Ella(Geometry) = 4 enrollment rows

	// ============================================================
	// === NULL PROPAGATION THROUGH OPERATIONS ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v49_nullable (
		id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, c TEXT)`)
	afExec(t, db, ctx, "INSERT INTO v49_nullable VALUES (1, 10, 20, 'hello')")
	afExec(t, db, ctx, "INSERT INTO v49_nullable VALUES (2, NULL, 30, 'world')")
	afExec(t, db, ctx, "INSERT INTO v49_nullable VALUES (3, 15, NULL, NULL)")
	afExec(t, db, ctx, "INSERT INTO v49_nullable VALUES (4, NULL, NULL, NULL)")

	// NP1: NULL in arithmetic
	check("NP1 NULL arithmetic",
		`SELECT a + b FROM v49_nullable WHERE id = 2`,
		"<nil>") // NULL + 30 = NULL

	// NP2: NULL in string concatenation
	check("NP2 NULL concat",
		`SELECT COALESCE(c, 'none') FROM v49_nullable WHERE id = 3`,
		"none") // c is NULL

	// NP3: NULL comparison
	check("NP3 NULL not equal",
		`SELECT COUNT(*) FROM v49_nullable WHERE a = NULL`,
		0) // Nothing equals NULL

	// NP4: NULL in CASE
	check("NP4 NULL in CASE",
		`SELECT CASE WHEN a IS NULL THEN 'missing' ELSE 'present' END
		 FROM v49_nullable WHERE id = 2`,
		"missing")

	// NP5: NULL in IN list
	check("NP5 NULL in IN",
		`SELECT COUNT(*) FROM v49_nullable WHERE a IN (10, 15)`,
		2) // ids 1 and 3, NULLs excluded

	// NP6: Aggregate ignores NULL
	check("NP6 AVG ignores NULL",
		`SELECT AVG(a) FROM v49_nullable`,
		12.5) // (10+15)/2 = 12.5

	// NP7: COUNT(*) vs COUNT(col) with NULLs
	check("NP7 COUNT col",
		`SELECT COUNT(a) FROM v49_nullable`,
		2) // Only 10 and 15

	// ============================================================
	// === COMPLEX GROUP BY ===
	// ============================================================

	// GB1: GROUP BY expression (honor: Amy,Cathy,Ella=3; regular: Ben,Dan,Fred=3 - tied)
	check("GB1 GROUP BY expression",
		`SELECT CASE WHEN gpa >= 3.5 THEN 'honor' ELSE 'regular' END AS tier,
		        COUNT(*) AS cnt
		 FROM v49_students
		 GROUP BY CASE WHEN gpa >= 3.5 THEN 'honor' ELSE 'regular' END
		 ORDER BY tier LIMIT 1`,
		"honor") // Alphabetical order for deterministic result

	// GB2: Multi-column GROUP BY
	check("GB2 multi-column GROUP BY",
		`SELECT COUNT(*) FROM (
		   SELECT t.subject, c.period FROM v49_teachers t
		   JOIN v49_classes c ON t.id = c.teacher_id
		   GROUP BY t.subject, c.period
		 ) AS groups`,
		5) // (Math,1),(Math,3),(Science,2),(Math,4),(English,1) - Science teachers share period 2

	// GB3: GROUP BY with HAVING and ORDER BY
	check("GB3 GROUP BY HAVING ORDER BY",
		`SELECT s.name, AVG(e.score) AS avg_score
		 FROM v49_students s
		 JOIN v49_enrollments e ON s.id = e.student_id
		 GROUP BY s.name
		 HAVING AVG(e.score) >= 88
		 ORDER BY avg_score DESC LIMIT 1`,
		"Cathy") // Cathy: (98+91)/2=94.5

	// ============================================================
	// === AGGREGATE EXPRESSIONS IN ORDER BY ===
	// ============================================================

	// AO1: ORDER BY aggregate
	check("AO1 ORDER BY SUM",
		`SELECT t.name FROM v49_teachers t
		 JOIN v49_classes c ON t.id = c.teacher_id
		 JOIN v49_enrollments e ON c.id = e.class_id
		 GROUP BY t.name
		 ORDER BY SUM(e.score) DESC LIMIT 1`,
		"Smith") // Smith: 95+98+78+70=341

	// AO2: ORDER BY COUNT
	check("AO2 ORDER BY COUNT",
		`SELECT s.name FROM v49_students s
		 JOIN v49_enrollments e ON s.id = e.student_id
		 GROUP BY s.name
		 ORDER BY COUNT(*) DESC LIMIT 1`,
		"Amy") // Amy has 3 enrollments

	// ============================================================
	// === SUBQUERY IN VARIOUS POSITIONS ===
	// ============================================================

	// SQ1: Subquery in WHERE >
	check("SQ1 subquery WHERE",
		`SELECT name FROM v49_students
		 WHERE gpa > (SELECT AVG(gpa) FROM v49_students)
		 ORDER BY gpa DESC LIMIT 1`,
		"Cathy") // avg gpa=3.383, Cathy=3.9

	// SQ2: Subquery in SELECT (scalar)
	check("SQ2 scalar subquery",
		`SELECT name, (SELECT MAX(score) FROM v49_enrollments e WHERE e.student_id = s.id) AS best_score
		 FROM v49_students s WHERE id = 1`,
		"Amy") // Amy, best score=95

	// SQ3: Correlated EXISTS
	check("SQ3 correlated EXISTS",
		`SELECT COUNT(*) FROM v49_teachers t
		 WHERE EXISTS (
		   SELECT 1 FROM v49_classes c
		   JOIN v49_enrollments e ON c.id = e.class_id
		   WHERE c.teacher_id = t.id AND e.score >= 90
		 )`,
		4) // Smith(95,98), Jones(90), Brown(91), Davis(92)

	// SQ4: Subquery in FROM with JOIN
	check("SQ4 subquery FROM JOIN",
		`SELECT ds.name FROM
		   (SELECT id, name FROM v49_students WHERE gpa >= 3.5) AS ds
		   JOIN v49_enrollments e ON ds.id = e.student_id
		 GROUP BY ds.name
		 ORDER BY COUNT(*) DESC LIMIT 1`,
		"Amy") // Amy(3 enrollments), Cathy(2), Ella(2)

	// ============================================================
	// === LIKE PATTERNS ===
	// ============================================================

	// LP1: LIKE with %
	checkRowCount("LP1 LIKE percent",
		`SELECT * FROM v49_teachers WHERE name LIKE 'S%'`,
		1) // Smith

	// LP2: LIKE with _ single char
	checkRowCount("LP2 LIKE underscore",
		`SELECT * FROM v49_students WHERE name LIKE '___'`,
		3) // Amy, Ben, Dan (3-char names)

	// LP3: LIKE not matching
	checkRowCount("LP3 LIKE no match",
		`SELECT * FROM v49_teachers WHERE name LIKE 'X%'`,
		0)

	// ============================================================
	// === INSERT INTO...SELECT WITH JOIN ===
	// ============================================================

	afExec(t, db, ctx, `CREATE TABLE v49_report (
		student TEXT, teacher TEXT, score INTEGER)`)

	checkNoError("IS1 INSERT SELECT JOIN",
		`INSERT INTO v49_report
		 SELECT s.name, t.name, e.score
		 FROM v49_students s
		 JOIN v49_enrollments e ON s.id = e.student_id
		 JOIN v49_classes c ON e.class_id = c.id
		 JOIN v49_teachers t ON c.teacher_id = t.id`)

	check("IS1 verify",
		`SELECT COUNT(*) FROM v49_report`,
		12)

	check("IS1 verify data",
		`SELECT teacher FROM v49_report WHERE student = 'Amy' AND score = 95`,
		"Smith")

	// ============================================================
	// === UPDATE WITH SUBQUERY IN WHERE ===
	// ============================================================

	// Make a copy table for updates
	afExec(t, db, ctx, `CREATE TABLE v49_student_copy (
		id INTEGER PRIMARY KEY, name TEXT, grade INTEGER, gpa REAL)`)
	afExec(t, db, ctx, `INSERT INTO v49_student_copy SELECT * FROM v49_students`)

	checkNoError("UW1 UPDATE subquery WHERE",
		`UPDATE v49_student_copy SET gpa = 4.0
		 WHERE id IN (SELECT student_id FROM v49_enrollments WHERE score >= 95)`)

	check("UW1 verify Amy",
		`SELECT gpa FROM v49_student_copy WHERE name = 'Amy'`,
		4) // Amy had score 95

	check("UW1 verify Cathy",
		`SELECT gpa FROM v49_student_copy WHERE name = 'Cathy'`,
		4) // Cathy had score 98

	check("UW1 verify Ben unchanged",
		`SELECT gpa FROM v49_student_copy WHERE name = 'Ben'`,
		3.2) // Ben's max was 85

	// ============================================================
	// === DELETE WITH SUBQUERY IN WHERE ===
	// ============================================================

	checkNoError("DW1 DELETE subquery WHERE",
		`DELETE FROM v49_student_copy
		 WHERE id NOT IN (SELECT DISTINCT student_id FROM v49_enrollments)`)

	check("DW1 verify all have enrollments",
		`SELECT COUNT(*) FROM v49_student_copy`,
		6) // All students have enrollments

	// ============================================================
	// === COMPLEX REAL-WORLD QUERIES ===
	// ============================================================

	// RW1: Teacher effectiveness report
	check("RW1 teacher effectiveness",
		`WITH teacher_stats AS (
		   SELECT t.name AS teacher_name,
		          COUNT(e.student_id) AS student_count,
		          AVG(e.score) AS avg_score
		   FROM v49_teachers t
		   JOIN v49_classes c ON t.id = c.teacher_id
		   JOIN v49_enrollments e ON c.id = e.class_id
		   GROUP BY t.name
		 )
		 SELECT teacher_name FROM teacher_stats
		 ORDER BY avg_score DESC LIMIT 1`,
		"Jones") // Jones(Physics): (88+90)/2=89 ties Brown(Geometry): (91+87)/2=89, Jones first by id

	// RW2: Student ranking by avg score (using CTE for clarity)
	check("RW2 student ranking",
		`WITH student_avgs AS (
		   SELECT s.name, AVG(e.score) AS avg_score
		   FROM v49_students s
		   JOIN v49_enrollments e ON s.id = e.student_id
		   GROUP BY s.name
		 )
		 SELECT name FROM student_avgs
		 ORDER BY avg_score DESC LIMIT 1`,
		"Cathy") // Cathy: (98+91)/2=94.5

	// RW3: Class popularity (most enrolled)
	check("RW3 class popularity",
		`SELECT c.subject FROM v49_classes c
		 JOIN v49_enrollments e ON c.id = e.class_id
		 GROUP BY c.subject
		 ORDER BY COUNT(*) DESC LIMIT 1`,
		"Algebra") // Amy, Cathy = 2

	t.Logf("\n=== V49 SQL CONFORMANCE: %d/%d tests passed ===", pass, total)
	if pass < total {
		t.Errorf("Some tests failed: %d/%d", pass, total)
	}
}
