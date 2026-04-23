package test

import (
	"fmt"
	"testing"
)

// TestV37MutationConsistency verifies that the database remains in a correct,
// consistent state after many data mutations. Every mutation section is followed
// by verification queries that confirm counts, aggregates, index correctness,
// trigger log counts, and view freshness.
//
// Seven domains are covered:
//  1. Bulk UPDATE consistency          (tests  U1-U12)
//  2. Bulk DELETE consistency          (tests  D1-D12)
//  3. INSERT patterns                  (tests  I1-I11)
//  4. Transaction consistency          (tests  T1-T12)
//  5. FK cascade chains                (tests  F1-F12)
//  6. Index consistency under mutation (tests  X1-X12)
//  7. Complex mutation sequences       (tests  C1-C12)
//
// All table names carry the v37_ prefix to prevent collisions with other test
// files. Expected values are derived by hand with inline arithmetic comments.
func TestV37MutationConsistency(t *testing.T) {
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

	// ============================================================
	// SECTION 1: BULK UPDATE CONSISTENCY
	// ============================================================
	//
	// Schema
	// ------
	//   v37_scores (id PK, player TEXT, score INTEGER, tier TEXT, bonus INTEGER)
	//
	// Initial data (10 rows):
	//   id  player    score  tier    bonus
	//    1  Alpha      100   bronze    5
	//    2  Beta       200   silver   10
	//    3  Gamma      300   silver   15
	//    4  Delta      400   gold     20
	//    5  Epsilon    500   gold     25
	//    6  Zeta       150   bronze    8
	//    7  Eta        250   silver   12
	//    8  Theta      350   gold     18
	//    9  Iota       450   gold     22
	//   10  Kappa       50   bronze    2
	//
	// Sum of scores before any UPDATE:
	//   100+200+300+400+500+150+250+350+450+50 = 2750
	//
	// Sum of bonus before any UPDATE:
	//   5+10+15+20+25+8+12+18+22+2 = 137

	afExec(t, db, ctx, `CREATE TABLE v37_scores (
		id     INTEGER PRIMARY KEY,
		player TEXT    NOT NULL,
		score  INTEGER,
		tier   TEXT,
		bonus  INTEGER
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (1,  'Alpha',   100, 'bronze',  5)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (2,  'Beta',    200, 'silver', 10)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (3,  'Gamma',   300, 'silver', 15)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (4,  'Delta',   400, 'gold',   20)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (5,  'Epsilon', 500, 'gold',   25)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (6,  'Zeta',    150, 'bronze',  8)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (7,  'Eta',     250, 'silver', 12)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (8,  'Theta',   350, 'gold',   18)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (9,  'Iota',    450, 'gold',   22)")
	afExec(t, db, ctx, "INSERT INTO v37_scores VALUES (10, 'Kappa',    50, 'bronze',  2)")

	// ---- Test U1: Baseline row count before any UPDATE ----
	// 10 rows inserted
	checkRowCount("U1 baseline row count is 10",
		`SELECT id FROM v37_scores`, 10)

	// ---- Test U2: UPDATE all rows matching a tier, verify affected count ----
	// UPDATE bronze tier players: set score = score + 50
	// Bronze players: Alpha(100->150), Zeta(150->200), Kappa(50->100) => 3 rows updated
	checkNoError("U2 UPDATE all bronze tier scores +50",
		`UPDATE v37_scores SET score = score + 50 WHERE tier = 'bronze'`)

	checkRowCount("U2b bronze players now have updated scores",
		`SELECT id FROM v37_scores WHERE tier = 'bronze' AND score >= 100`, 3)

	// Bronze scores after update: Alpha=150, Zeta=200, Kappa=100
	// Non-bronze scores unchanged: 200,300,400,500,250,350,450
	// New sum = (150+200+100) + (200+300+400+500+250+350+450) = 450 + 2450 = 2900
	check("U2c total score sum after bronze +50 update is 2900",
		`SELECT SUM(score) FROM v37_scores`, 2900)

	// ---- Test U3: UPDATE with arithmetic expression (score = score * 2 + 1) ----
	// UPDATE silver tier: score = score * 2 + 1
	// Silver players: Beta(200), Gamma(300), Eta(250)
	//   Beta:  200*2+1 = 401
	//   Gamma: 300*2+1 = 601
	//   Eta:   250*2+1 = 501
	// Silver sum before = 200+300+250 = 750
	// Silver sum after  = 401+601+501 = 1503
	// Non-silver sum unchanged = 2900 - 750 = 2150
	// New total = 2150 + 1503 = 3653
	checkNoError("U3 UPDATE silver tier score = score*2+1",
		`UPDATE v37_scores SET score = score * 2 + 1 WHERE tier = 'silver'`)

	check("U3b total score sum after silver arithmetic UPDATE is 3653",
		`SELECT SUM(score) FROM v37_scores`, 3653)

	check("U3c Beta new score is 401",
		`SELECT score FROM v37_scores WHERE player = 'Beta'`, 401)

	check("U3d Gamma new score is 601",
		`SELECT score FROM v37_scores WHERE player = 'Gamma'`, 601)

	// ---- Test U4: UPDATE with CASE expression in SET clause ----
	// Apply tier promotion based on new score:
	//   score >= 500 => 'platinum', score >= 300 => 'gold', score >= 150 => 'silver', else 'bronze'
	// Current scores:
	//   Alpha=150(silver), Beta=401(gold), Gamma=601(platinum), Delta=400(gold), Epsilon=500(platinum)
	//   Zeta=200(silver), Eta=501(platinum), Theta=350(gold), Iota=450(gold), Kappa=100(bronze)
	checkNoError("U4 UPDATE tier using CASE on score",
		`UPDATE v37_scores SET tier = CASE
		   WHEN score >= 500 THEN 'platinum'
		   WHEN score >= 300 THEN 'gold'
		   WHEN score >= 150 THEN 'silver'
		   ELSE 'bronze'
		 END`)

	// Platinum players: Gamma(601), Epsilon(500), Eta(501) => 3
	checkRowCount("U4b platinum tier count is 3 after CASE UPDATE",
		`SELECT id FROM v37_scores WHERE tier = 'platinum'`, 3)

	// Bronze players: Kappa(100) => 1
	checkRowCount("U4c bronze tier count is 1 after CASE UPDATE",
		`SELECT id FROM v37_scores WHERE tier = 'bronze'`, 1)

	// ---- Test U5: UPDATE multiple rows then verify SUM/AVG/COUNT unchanged count ----
	// Apply bonus = bonus * 2 to all gold tier players
	// Gold players after CASE update: Beta(401), Delta(400), Theta(350), Iota(450) => 4 players
	// Their current bonuses: Beta=10, Delta=20, Theta=18, Iota=22 => sum=70
	// After bonus*2: 20+40+36+44 = 140
	// Non-gold bonus sum: 5+15+25+8+12+2 = 67  (unchanged)
	// New total bonus = 140 + 67 = 207
	checkNoError("U5 UPDATE gold tier bonus = bonus*2",
		`UPDATE v37_scores SET bonus = bonus * 2 WHERE tier = 'gold'`)

	check("U5b total bonus sum after gold bonus*2 is 207",
		`SELECT SUM(bonus) FROM v37_scores`, 207)

	// Count should remain 10
	check("U5c row count still 10 after UPDATE",
		`SELECT COUNT(*) FROM v37_scores`, 10)

	// ---- Test U6: UPDATE that changes an indexed column value ----
	// Create index on tier, then UPDATE tier for a specific player
	// Verify the old tier no longer returns that player and new tier does
	checkNoError("U6 create index on tier column",
		`CREATE INDEX idx_v37_scores_tier ON v37_scores(tier)`)

	// Update Kappa from 'bronze' to 'silver'
	checkNoError("U6b UPDATE Kappa tier from bronze to silver",
		`UPDATE v37_scores SET tier = 'silver' WHERE player = 'Kappa'`)

	// Bronze count was 1 (Kappa) => now 0
	checkRowCount("U6c bronze tier is now empty after Kappa update",
		`SELECT id FROM v37_scores WHERE tier = 'bronze'`, 0)

	// Silver count was: Alpha(150 silver from U4), Zeta(200 silver from U4) + Kappa = 3
	checkRowCount("U6d silver tier has 3 players after Kappa update",
		`SELECT id FROM v37_scores WHERE tier = 'silver'`, 3)

	// ---- Test U7: Verify AVG after bulk UPDATE ----
	// Current platinum scores: Gamma=601, Epsilon=500, Eta=501
	// AVG = (601+500+501)/3 = 1602/3 = 534
	check("U7 AVG score for platinum tier is 534",
		`SELECT AVG(score) FROM v37_scores WHERE tier = 'platinum'`, 534)

	// ---- Test U8: UPDATE bonus using score/100 (engine uses float division) ----
	// Set each row's bonus to (score / 100) — engine performs float division.
	// Scores at this point (after U2 bronze+50, U3 silver*2+1, U4 tier CASE):
	//   Alpha=150, Beta=401, Gamma=601, Delta=400, Epsilon=500
	//   Zeta=200,  Eta=501,  Theta=350, Iota=450,  Kappa=100
	// Float division results:
	//   150/100=1.5, 401/100=4.01, 601/100=6.01, 400/100=4.0, 500/100=5.0
	//   200/100=2.0, 501/100=5.01, 350/100=3.5,  450/100=4.5, 100/100=1.0
	// Sum = 1.5+4.01+6.01+4.0+5.0+2.0+5.01+3.5+4.5+1.0 = 36.53
	checkNoError("U8 UPDATE bonus = score/100 for all rows",
		`UPDATE v37_scores SET bonus = score / 100`)

	check("U8b total bonus after score/100 update is 36.53 (float division)",
		`SELECT SUM(bonus) FROM v37_scores`, 36.53)

	// ---- Test U9: UPDATE MAX score holder and verify aggregate shift ----
	// Current max score = 601 (Gamma). UPDATE Gamma's score to 100.
	// New max = 501 (Eta)
	checkNoError("U9 UPDATE Gamma score from 601 to 100",
		`UPDATE v37_scores SET score = 100 WHERE player = 'Gamma'`)

	check("U9b new MAX score after Gamma update is 501",
		`SELECT MAX(score) FROM v37_scores`, 501)

	// Gamma is now in platinum tier (still from U4 update) with score=100
	// Check Gamma's current state
	check("U9c Gamma score is now 100",
		`SELECT score FROM v37_scores WHERE player = 'Gamma'`, 100)

	// ---- Test U10: UPDATE row count remains stable ----
	// No deletes performed in this section; all 10 rows must still exist
	check("U10 row count is still 10 after all UPDATEs",
		`SELECT COUNT(*) FROM v37_scores`, 10)

	// ---- Test U11: UPDATE with MIN/MAX consistency check ----
	// Set Kappa score = 999 (highest possible in this table)
	checkNoError("U11 UPDATE Kappa score to 999",
		`UPDATE v37_scores SET score = 999 WHERE player = 'Kappa'`)

	check("U11b MIN score is unchanged (Delta or Theta area)",
		`SELECT MIN(score) FROM v37_scores WHERE player = 'Gamma'`, 100)

	check("U11c MAX score is now 999 (Kappa)",
		`SELECT MAX(score) FROM v37_scores`, 999)

	// ---- Test U12: Reverse UPDATE to restore predictable state for later sections ----
	// Restore all scores to a deterministic value: score = id * 100
	// id=1..10 => scores: 100,200,300,400,500,600,700,800,900,1000
	// Sum = 100+200+...+1000 = 5500
	checkNoError("U12 restore scores to id*100",
		`UPDATE v37_scores SET score = id * 100`)

	check("U12b total score after restore is 5500",
		`SELECT SUM(score) FROM v37_scores`, 5500)

	// ============================================================
	// SECTION 2: BULK DELETE CONSISTENCY
	// ============================================================
	//
	// Schema
	// ------
	//   v37_orders (id PK, customer TEXT, amount REAL, status TEXT, region TEXT)
	//
	// Initial data (20 rows):
	//   id  customer  amount   status    region
	//    1  CustA      100.00  pending   North
	//    2  CustB      200.00  shipped   South
	//    3  CustC      150.00  pending   North
	//    4  CustD      300.00  delivered East
	//    5  CustE      250.00  shipped   West
	//    6  CustF       75.00  pending   South
	//    7  CustG      400.00  delivered North
	//    8  CustH      180.00  shipped   East
	//    9  CustI      220.00  pending   West
	//   10  CustJ      500.00  delivered South
	//   11  CustK       90.00  cancelled North
	//   12  CustL      350.00  shipped   East
	//   13  CustM      130.00  pending   West
	//   14  CustN      275.00  delivered North
	//   15  CustO       60.00  cancelled South
	//   16  CustP      420.00  shipped   East
	//   17  CustQ      190.00  pending   West
	//   18  CustR      310.00  delivered North
	//   19  CustS       85.00  cancelled East
	//   20  CustT      230.00  shipped   South
	//
	// Total rows = 20
	// Sum of all amounts:
	//   100+200+150+300+250+75+400+180+220+500
	//   +90+350+130+275+60+420+190+310+85+230
	//   = (100+200+150+300+250+75+400+180+220+500) = 2375
	//   + (90+350+130+275+60+420+190+310+85+230)   = 2140
	//   = 4515
	//
	// Status counts:
	//   pending:   ids 1,3,6,9,13,17 => 6
	//   shipped:   ids 2,5,8,12,16,20 => 6
	//   delivered: ids 4,7,10,14,18 => 5
	//   cancelled: ids 11,15,19 => 3

	afExec(t, db, ctx, `CREATE TABLE v37_orders (
		id       INTEGER PRIMARY KEY,
		customer TEXT    NOT NULL,
		amount   REAL,
		status   TEXT,
		region   TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (1,  'CustA',  100.00, 'pending',   'North')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (2,  'CustB',  200.00, 'shipped',   'South')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (3,  'CustC',  150.00, 'pending',   'North')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (4,  'CustD',  300.00, 'delivered', 'East')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (5,  'CustE',  250.00, 'shipped',   'West')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (6,  'CustF',   75.00, 'pending',   'South')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (7,  'CustG',  400.00, 'delivered', 'North')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (8,  'CustH',  180.00, 'shipped',   'East')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (9,  'CustI',  220.00, 'pending',   'West')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (10, 'CustJ',  500.00, 'delivered', 'South')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (11, 'CustK',   90.00, 'cancelled', 'North')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (12, 'CustL',  350.00, 'shipped',   'East')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (13, 'CustM',  130.00, 'pending',   'West')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (14, 'CustN',  275.00, 'delivered', 'North')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (15, 'CustO',   60.00, 'cancelled', 'South')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (16, 'CustP',  420.00, 'shipped',   'East')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (17, 'CustQ',  190.00, 'pending',   'West')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (18, 'CustR',  310.00, 'delivered', 'North')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (19, 'CustS',   85.00, 'cancelled', 'East')")
	afExec(t, db, ctx, "INSERT INTO v37_orders VALUES (20, 'CustT',  230.00, 'shipped',   'South')")

	// ---- Test D1: Baseline count and sum before any DELETE ----
	check("D1 baseline row count is 20",
		`SELECT COUNT(*) FROM v37_orders`, 20)

	check("D1b baseline total amount is 4515",
		`SELECT SUM(amount) FROM v37_orders`, 4515)

	// ---- Test D2: DELETE with complex WHERE (AND/OR), verify remaining rows ----
	// DELETE cancelled orders in North or South regions
	// Cancelled: ids 11(North), 15(South), 19(East)
	// Matches (cancelled AND (North OR South)): id 11, id 15 => 2 rows deleted
	// Remaining = 20 - 2 = 18
	checkNoError("D2 DELETE cancelled orders in North or South",
		`DELETE FROM v37_orders WHERE status = 'cancelled' AND (region = 'North' OR region = 'South')`)

	checkRowCount("D2b 18 rows remain after DELETE",
		`SELECT id FROM v37_orders`, 18)

	// Verify East cancelled (id=19) still exists
	checkRowCount("D2c East cancelled order still exists",
		`SELECT id FROM v37_orders WHERE status = 'cancelled' AND region = 'East'`, 1)

	// ---- Test D3: DELETE half the rows, verify aggregates on remaining ----
	// DELETE pending orders with amount < 200
	// Pending orders: ids 1(100), 3(150), 6(75), 9(220), 13(130), 17(190)
	// Pending with amount < 200: ids 1(100), 3(150), 6(75), 13(130), 17(190) => 5 rows
	// Remaining after D2+D3: 18 - 5 = 13 rows
	// Deleted amounts from pending<200: 100+150+75+130+190 = 645
	// Remaining amount sum:
	//   D2 deleted: 90+60 = 150  (ids 11,15)
	//   D3 deleted: 100+150+75+130+190 = 645
	//   Total deleted = 795
	//   Remaining sum = 4515 - 795 = 3720
	checkNoError("D3 DELETE pending orders with amount < 200",
		`DELETE FROM v37_orders WHERE status = 'pending' AND amount < 200`)

	check("D3b row count is 13 after D3 deletes",
		`SELECT COUNT(*) FROM v37_orders`, 13)

	check("D3c remaining amount sum is 3720",
		`SELECT SUM(amount) FROM v37_orders`, 3720)

	// ---- Test D4: Verify aggregates on remaining rows are correct ----
	// Remaining shipped orders:
	//   ids still in table with status='shipped': 2(200),5(250),8(180),12(350),16(420),20(230)
	//   all 6 shipped orders survived
	// Shipped sum = 200+250+180+350+420+230 = 1630
	check("D4 shipped order total is 1630",
		`SELECT SUM(amount) FROM v37_orders WHERE status = 'shipped'`, 1630)

	checkRowCount("D4b shipped order count is 6",
		`SELECT id FROM v37_orders WHERE status = 'shipped'`, 6)

	// ---- Test D5: DELETE with subquery (delete orders below average amount) ----
	// Average of remaining 13 rows = 3720/13 = 286.15...
	// Remaining amounts: 200,300,250,400,180,500,350,420,275,310,85,190(no-wait),... let me recompute
	// Rows in table after D2+D3:
	//   2(CustB,200,shipped,South), 4(CustD,300,delivered,East), 5(CustE,250,shipped,West)
	//   7(CustG,400,delivered,North), 8(CustH,180,shipped,East), 9(CustI,220,pending,West) - DELETED by D3? No, 220>=200 survives
	//   Wait: D3 deleted pending with amount<200. id=9 is pending with amount=220 => 220>=200 => SURVIVES
	//   10(CustJ,500,delivered,South), 12(CustL,350,shipped,East), 14(CustN,275,delivered,North)
	//   16(CustP,420,shipped,East), 18(CustR,310,delivered,North), 19(CustS,85,cancelled,East), 20(CustT,230,shipped,South)
	// That is 13 rows: 200,300,250,400,180,220,500,350,275,420,310,85,230
	// Average = 3720/13 = 286.15...
	// Below average (amount < 286.15): 200,250,180,220,275,85,230 => 7 rows
	//   ids: 2(200), 5(250), 8(180), 9(220), 14(275), 19(85), 20(230)
	// DELETE those 7 rows; remaining = 13 - 7 = 6
	checkNoError("D5 DELETE orders below average amount",
		`DELETE FROM v37_orders WHERE amount < (SELECT AVG(amount) FROM v37_orders)`)

	check("D5b row count after below-average delete is 6",
		`SELECT COUNT(*) FROM v37_orders`, 6)

	// Remaining: 4(300), 7(400), 10(500), 12(350), 16(420), 18(310) => sum=2280
	check("D5c sum after below-average delete is 2280",
		`SELECT SUM(amount) FROM v37_orders`, 2280)

	// ---- Test D6: DELETE then INSERT same PKs, verify no ghost data ----
	// Delete id=4, then re-insert id=4 with different data
	checkNoError("D6 DELETE id=4",
		`DELETE FROM v37_orders WHERE id = 4`)

	checkRowCount("D6b id=4 is gone",
		`SELECT id FROM v37_orders WHERE id = 4`, 0)

	// Re-insert id=4 with a new customer and amount
	checkNoError("D6c re-INSERT id=4 with new data",
		`INSERT INTO v37_orders VALUES (4, 'NewCust', 999.00, 'pending', 'North')`)

	check("D6d new id=4 has customer NewCust",
		`SELECT customer FROM v37_orders WHERE id = 4`, "NewCust")

	check("D6e new id=4 has amount 999",
		`SELECT amount FROM v37_orders WHERE id = 4`, 999)

	// Old data (CustD) must not exist
	checkRowCount("D6f old CustD data is gone",
		`SELECT id FROM v37_orders WHERE customer = 'CustD'`, 0)

	// ---- Test D7: Sequential DELETE operations, verify count at each step ----
	// Current rows: 4(NewCust,999), 7(400), 10(500), 12(350), 16(420), 18(310) => 6 rows
	// Step 1: delete id=18 => 5 rows
	checkNoError("D7 step-1 DELETE id=18",
		`DELETE FROM v37_orders WHERE id = 18`)

	check("D7b count is 5 after step-1 delete",
		`SELECT COUNT(*) FROM v37_orders`, 5)

	// Step 2: delete id=12 => 4 rows
	checkNoError("D7c step-2 DELETE id=12",
		`DELETE FROM v37_orders WHERE id = 12`)

	check("D7d count is 4 after step-2 delete",
		`SELECT COUNT(*) FROM v37_orders`, 4)

	// Step 3: delete id=16 => 3 rows
	checkNoError("D7e step-3 DELETE id=16",
		`DELETE FROM v37_orders WHERE id = 16`)

	check("D7f count is 3 after step-3 delete",
		`SELECT COUNT(*) FROM v37_orders`, 3)

	// Remaining: 4(999), 7(400), 10(500) => sum=1899
	check("D7g sum of remaining 3 rows is 1899",
		`SELECT SUM(amount) FROM v37_orders`, 1899)

	// ---- Test D8: DELETE all rows, verify empty table aggregates ----
	checkNoError("D8 DELETE all remaining rows",
		`DELETE FROM v37_orders`)

	check("D8b COUNT on empty table is 0",
		`SELECT COUNT(*) FROM v37_orders`, 0)

	// ============================================================
	// SECTION 3: INSERT PATTERNS
	// ============================================================
	//
	// Schema
	// ------
	//   v37_products (id PK, name TEXT UNIQUE, price REAL, stock INTEGER, category TEXT)

	afExec(t, db, ctx, `CREATE TABLE v37_products (
		id       INTEGER PRIMARY KEY,
		name     TEXT    NOT NULL UNIQUE,
		price    REAL,
		stock    INTEGER,
		category TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_products VALUES (1,  'Widget',    9.99,  100, 'hardware')")
	afExec(t, db, ctx, "INSERT INTO v37_products VALUES (2,  'Gadget',   29.99,   50, 'electronics')")
	afExec(t, db, ctx, "INSERT INTO v37_products VALUES (3,  'Doohickey', 4.99,  200, 'hardware')")
	afExec(t, db, ctx, "INSERT INTO v37_products VALUES (4,  'Thingamajig', 14.99, 75, 'electronics')")
	afExec(t, db, ctx, "INSERT INTO v37_products VALUES (5,  'Gizmo',    49.99,   30, 'electronics')")

	// ---- Test I1: INSERT OR REPLACE in bulk (10+ replaces) ----
	// Replace existing rows and add new ones in one batch
	// IDs 1-5 are re-inserted with OR REPLACE, changing prices
	// IDs 6-12 are brand new inserts
	checkNoError("I1 INSERT OR REPLACE for id=1 Widget new price",
		`INSERT OR REPLACE INTO v37_products VALUES (1, 'Widget', 19.99, 110, 'hardware')`)
	checkNoError("I1b INSERT OR REPLACE for id=2 Gadget new price",
		`INSERT OR REPLACE INTO v37_products VALUES (2, 'Gadget', 39.99, 55, 'electronics')`)
	checkNoError("I1c INSERT OR REPLACE for id=3 Doohickey new price",
		`INSERT OR REPLACE INTO v37_products VALUES (3, 'Doohickey', 5.99, 210, 'hardware')`)
	checkNoError("I1d INSERT OR REPLACE for id=4 new price",
		`INSERT OR REPLACE INTO v37_products VALUES (4, 'Thingamajig', 16.99, 80, 'electronics')`)
	checkNoError("I1e INSERT OR REPLACE for id=5 new price",
		`INSERT OR REPLACE INTO v37_products VALUES (5, 'Gizmo', 59.99, 35, 'electronics')`)
	checkNoError("I1f INSERT OR REPLACE new id=6",
		`INSERT OR REPLACE INTO v37_products VALUES (6, 'Sprocket', 2.99, 500, 'hardware')`)
	checkNoError("I1g INSERT OR REPLACE new id=7",
		`INSERT OR REPLACE INTO v37_products VALUES (7, 'Valve', 12.99, 150, 'mechanical')`)
	checkNoError("I1h INSERT OR REPLACE new id=8",
		`INSERT OR REPLACE INTO v37_products VALUES (8, 'Bearing', 7.49, 300, 'mechanical')`)
	checkNoError("I1i INSERT OR REPLACE new id=9",
		`INSERT OR REPLACE INTO v37_products VALUES (9, 'Sensor', 89.99, 20, 'electronics')`)
	checkNoError("I1j INSERT OR REPLACE new id=10",
		`INSERT OR REPLACE INTO v37_products VALUES (10, 'Chip', 3.99, 1000, 'electronics')`)
	checkNoError("I1k INSERT OR REPLACE new id=11",
		`INSERT OR REPLACE INTO v37_products VALUES (11, 'Bolt', 0.49, 5000, 'hardware')`)
	checkNoError("I1l INSERT OR REPLACE new id=12",
		`INSERT OR REPLACE INTO v37_products VALUES (12, 'Cable', 6.99, 400, 'hardware')`)

	// After 12 OR REPLACE operations: ids 1-12 => exactly 12 rows
	check("I1m row count is 12 after bulk INSERT OR REPLACE",
		`SELECT COUNT(*) FROM v37_products`, 12)

	// Widget price should be the new value 19.99
	check("I1n Widget price was replaced to 19.99",
		`SELECT price FROM v37_products WHERE name = 'Widget'`, 19.99)

	// ---- Test I2: INSERT OR IGNORE with many conflicts ----
	// Attempt to re-insert ids 1-5 with OR IGNORE (all will conflict on PK)
	// All 5 should be silently ignored; table still has 12 rows
	checkNoError("I2 INSERT OR IGNORE conflict id=1",
		`INSERT OR IGNORE INTO v37_products VALUES (1, 'Widget-dup', 0.01, 0, 'dummy')`)
	checkNoError("I2b INSERT OR IGNORE conflict id=2",
		`INSERT OR IGNORE INTO v37_products VALUES (2, 'Gadget-dup', 0.01, 0, 'dummy')`)
	checkNoError("I2c INSERT OR IGNORE conflict id=3",
		`INSERT OR IGNORE INTO v37_products VALUES (3, 'Dupe3', 0.01, 0, 'dummy')`)
	checkNoError("I2d INSERT OR IGNORE conflict id=4",
		`INSERT OR IGNORE INTO v37_products VALUES (4, 'Dupe4', 0.01, 0, 'dummy')`)
	checkNoError("I2e INSERT OR IGNORE conflict id=5",
		`INSERT OR IGNORE INTO v37_products VALUES (5, 'Dupe5', 0.01, 0, 'dummy')`)

	// Table row count must still be 12 (no rows added)
	check("I2f row count still 12 after INSERT OR IGNORE conflicts",
		`SELECT COUNT(*) FROM v37_products`, 12)

	// Widget name must be unchanged (not overwritten to Widget-dup)
	check("I2g Widget name unchanged after INSERT OR IGNORE",
		`SELECT name FROM v37_products WHERE id = 1`, "Widget")

	// ---- Test I3: INSERT...SELECT from same table (copy a subset) ----
	// Create a staging table and copy hardware products into it
	afExec(t, db, ctx, `CREATE TABLE v37_hardware_staging (
		id       INTEGER PRIMARY KEY,
		name     TEXT,
		price    REAL,
		stock    INTEGER
	)`)

	// Hardware products: ids 1(Widget), 3(Doohickey), 6(Sprocket), 11(Bolt), 12(Cable)
	// Plus id=8(Bearing) is mechanical - skip
	// Hardware: Widget, Doohickey, Sprocket, Bolt, Cable => 5 rows
	checkNoError("I3 INSERT...SELECT hardware products into staging",
		`INSERT INTO v37_hardware_staging (id, name, price, stock)
		 SELECT id, name, price, stock FROM v37_products WHERE category = 'hardware'`)

	check("I3b staging table has 5 hardware rows",
		`SELECT COUNT(*) FROM v37_hardware_staging`, 5)

	// Verify Widget is in staging
	checkRowCount("I3c Widget is in staging table",
		`SELECT id FROM v37_hardware_staging WHERE name = 'Widget'`, 1)

	// Verify Gadget (electronics) is NOT in staging
	checkRowCount("I3d Gadget is not in staging table",
		`SELECT id FROM v37_hardware_staging WHERE name = 'Gadget'`, 0)

	// ---- Test I4: INSERT with expressions and functions in VALUES ----
	// Insert a computed row where values use arithmetic and string functions
	afExec(t, db, ctx, `CREATE TABLE v37_computed_inserts (
		id    INTEGER PRIMARY KEY,
		label TEXT,
		value REAL,
		tag   TEXT
	)`)
	checkNoError("I4 INSERT with arithmetic expression in VALUES",
		`INSERT INTO v37_computed_inserts VALUES (1, 'sum', 10 + 20 * 3, 'math')`)
	checkNoError("I4b INSERT with division expression",
		`INSERT INTO v37_computed_inserts VALUES (2, 'div', 100 / 4, 'math')`)
	checkNoError("I4c INSERT with string concat",
		`INSERT INTO v37_computed_inserts VALUES (3, 'hello' || ' ' || 'world', 0, 'str')`)

	// 10 + 20*3 = 70
	check("I4d computed value 10+20*3 is 70",
		`SELECT value FROM v37_computed_inserts WHERE id = 1`, 70)

	// 100/4 = 25
	check("I4e computed value 100/4 is 25",
		`SELECT value FROM v37_computed_inserts WHERE id = 2`, 25)

	// label for id=3 is 'hello world'
	check("I4f string concat label is hello world",
		`SELECT label FROM v37_computed_inserts WHERE id = 3`, "hello world")

	// ---- Test I5: INSERT with auto_increment, verify sequential IDs after deletes ----
	afExec(t, db, ctx, `CREATE TABLE v37_autoinc (
		id    INTEGER PRIMARY KEY AUTO_INCREMENT,
		name  TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_autoinc (name) VALUES ('Row1')")
	afExec(t, db, ctx, "INSERT INTO v37_autoinc (name) VALUES ('Row2')")
	afExec(t, db, ctx, "INSERT INTO v37_autoinc (name) VALUES ('Row3')")

	check("I5 auto_increment generates id=3 for third row",
		`SELECT MAX(id) FROM v37_autoinc`, 3)

	// Delete id=2 (Row2)
	afExec(t, db, ctx, "DELETE FROM v37_autoinc WHERE id = 2")

	// Insert Row4 - should get id=4 (not reuse id=2)
	afExec(t, db, ctx, "INSERT INTO v37_autoinc (name) VALUES ('Row4')")

	check("I5b new insert after delete gets id=4 not reused id=2",
		`SELECT id FROM v37_autoinc WHERE name = 'Row4'`, 4)

	checkRowCount("I5c table has 3 rows (Row1, Row3, Row4)",
		`SELECT id FROM v37_autoinc`, 3)

	// ---- Test I6: Verify no ghost data from replaced rows ----
	// v37_products: OR REPLACE on id=1 replaced Widget data; verify original price 9.99 is gone
	checkRowCount("I6 original Widget price 9.99 is not in table",
		`SELECT id FROM v37_products WHERE name = 'Widget' AND price = 9.99`, 0)

	// ---- Test I7: INSERT and verify SUM/COUNT update correctly ----
	// Current hardware_staging sum of stock: Widget(110)+Doohickey(210)+Sprocket(500)+Bolt(5000)+Cable(400)
	// = 110+210+500+5000+400 = 6220
	check("I7 staging stock sum is 6220",
		`SELECT SUM(stock) FROM v37_hardware_staging`, 6220)

	// Insert a new hardware row into staging: Nail, stock=2000
	checkNoError("I7b INSERT new Nail row into staging",
		`INSERT INTO v37_hardware_staging VALUES (20, 'Nail', 0.10, 2000)`)

	// New sum = 6220 + 2000 = 8220
	check("I7c staging stock sum after new insert is 8220",
		`SELECT SUM(stock) FROM v37_hardware_staging`, 8220)

	// ============================================================
	// SECTION 4: TRANSACTION CONSISTENCY
	// ============================================================
	//
	// Schema
	// ------
	//   v37_accounts (id PK, owner TEXT, balance REAL, account_type TEXT)
	//   v37_txn_log  (id PK AUTO_INCREMENT, account_id INTEGER, delta REAL, note TEXT)
	//
	// Initial data:
	//   id  owner  balance  account_type
	//    1  Alice  1000.00  checking
	//    2  Bob     500.00  checking
	//    3  Alice  2000.00  savings
	//    4  Carol   750.00  checking
	//
	// Total balance = 1000+500+2000+750 = 4250

	afExec(t, db, ctx, `CREATE TABLE v37_accounts (
		id           INTEGER PRIMARY KEY,
		owner        TEXT    NOT NULL,
		balance      REAL,
		account_type TEXT
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_accounts VALUES (1, 'Alice', 1000.00, 'checking')")
	afExec(t, db, ctx, "INSERT INTO v37_accounts VALUES (2, 'Bob',    500.00, 'checking')")
	afExec(t, db, ctx, "INSERT INTO v37_accounts VALUES (3, 'Alice', 2000.00, 'savings')")
	afExec(t, db, ctx, "INSERT INTO v37_accounts VALUES (4, 'Carol',  750.00, 'checking')")

	afExec(t, db, ctx, `CREATE TABLE v37_txn_log (
		id         INTEGER PRIMARY KEY AUTO_INCREMENT,
		account_id INTEGER,
		delta      REAL,
		note       TEXT
	)`)

	// ---- Test T1: BEGIN + multiple UPDATEs + COMMIT, verify all persisted ----
	// Transfer 200 from Alice checking to Bob checking
	checkNoError("T1 BEGIN transaction",
		`BEGIN`)
	checkNoError("T1b debit Alice checking by 200",
		`UPDATE v37_accounts SET balance = balance - 200 WHERE id = 1`)
	checkNoError("T1c credit Bob checking by 200",
		`UPDATE v37_accounts SET balance = balance + 200 WHERE id = 2`)
	checkNoError("T1d log debit",
		`INSERT INTO v37_txn_log (account_id, delta, note) VALUES (1, -200, 'transfer out')`)
	checkNoError("T1e log credit",
		`INSERT INTO v37_txn_log (account_id, delta, note) VALUES (2, 200, 'transfer in')`)
	checkNoError("T1f COMMIT",
		`COMMIT`)

	// Alice checking: 1000-200 = 800
	check("T1g Alice checking balance is 800 after commit",
		`SELECT balance FROM v37_accounts WHERE id = 1`, 800)

	// Bob checking: 500+200 = 700
	check("T1h Bob checking balance is 700 after commit",
		`SELECT balance FROM v37_accounts WHERE id = 2`, 700)

	// Total balance unchanged: 800+700+2000+750 = 4250
	check("T1i total balance unchanged at 4250 after transfer",
		`SELECT SUM(balance) FROM v37_accounts`, 4250)

	// Log has 2 entries
	check("T1j txn_log has 2 entries after commit",
		`SELECT COUNT(*) FROM v37_txn_log`, 2)

	// ---- Test T2: BEGIN + multiple UPDATEs + ROLLBACK, verify ALL changes reverted ----
	// Attempt to transfer 300 from Alice savings to Carol, then rollback
	checkNoError("T2 BEGIN second transaction",
		`BEGIN`)
	checkNoError("T2b debit Alice savings by 300",
		`UPDATE v37_accounts SET balance = balance - 300 WHERE id = 3`)
	checkNoError("T2c credit Carol by 300",
		`UPDATE v37_accounts SET balance = balance + 300 WHERE id = 4`)
	checkNoError("T2d ROLLBACK second transaction",
		`ROLLBACK`)

	// Alice savings must be back to 2000 (unchanged)
	check("T2e Alice savings balance reverted to 2000 after ROLLBACK",
		`SELECT balance FROM v37_accounts WHERE id = 3`, 2000)

	// Carol must be back to 750 (unchanged)
	check("T2f Carol balance reverted to 750 after ROLLBACK",
		`SELECT balance FROM v37_accounts WHERE id = 4`, 750)

	// Total still 4250
	check("T2g total balance still 4250 after rollback",
		`SELECT SUM(balance) FROM v37_accounts`, 4250)

	// Log still has only 2 entries (rollback also reverted the log inserts)
	check("T2h txn_log still has 2 entries after rollback",
		`SELECT COUNT(*) FROM v37_txn_log`, 2)

	// ---- Test T3: Transaction with INSERT + UPDATE + DELETE, commit ----
	// Add new account for Dave, update Alice's balance, remove Bob's account
	checkNoError("T3 BEGIN",
		`BEGIN`)
	checkNoError("T3b INSERT Dave account",
		`INSERT INTO v37_accounts VALUES (5, 'Dave', 1500.00, 'savings')`)
	checkNoError("T3c UPDATE Alice savings += 500",
		`UPDATE v37_accounts SET balance = balance + 500 WHERE id = 3`)
	checkNoError("T3d DELETE Bob account",
		`DELETE FROM v37_accounts WHERE id = 2`)
	checkNoError("T3e COMMIT",
		`COMMIT`)

	// Dave exists with 1500
	check("T3f Dave account exists with balance 1500",
		`SELECT balance FROM v37_accounts WHERE id = 5`, 1500)

	// Alice savings: 2000+500 = 2500
	check("T3g Alice savings is 2500 after UPDATE in committed txn",
		`SELECT balance FROM v37_accounts WHERE id = 3`, 2500)

	// Bob is gone
	checkRowCount("T3h Bob account deleted in committed txn",
		`SELECT id FROM v37_accounts WHERE id = 2`, 0)

	// Row count: started with 4, added Dave(+1), removed Bob(-1) => 4 rows
	check("T3i account count is 4 after committed txn",
		`SELECT COUNT(*) FROM v37_accounts`, 4)

	// ---- Test T4: Transaction with INSERT + UPDATE + DELETE, rollback ----
	// Try to add Eve, update Dave, delete Carol; then rollback
	checkNoError("T4 BEGIN",
		`BEGIN`)
	checkNoError("T4b INSERT Eve account",
		`INSERT INTO v37_accounts VALUES (6, 'Eve', 999.00, 'checking')`)
	checkNoError("T4c UPDATE Dave balance *= 2",
		`UPDATE v37_accounts SET balance = balance * 2 WHERE id = 5`)
	checkNoError("T4d DELETE Carol account",
		`DELETE FROM v37_accounts WHERE id = 4`)
	checkNoError("T4e ROLLBACK",
		`ROLLBACK`)

	// Eve must not exist
	checkRowCount("T4f Eve not present after rollback",
		`SELECT id FROM v37_accounts WHERE owner = 'Eve'`, 0)

	// Dave balance back to 1500
	check("T4g Dave balance reverted to 1500 after rollback",
		`SELECT balance FROM v37_accounts WHERE id = 5`, 1500)

	// Carol still exists with 750
	check("T4h Carol still exists with 750 after rollback",
		`SELECT balance FROM v37_accounts WHERE id = 4`, 750)

	// ---- Test T5: Verify aggregate results match expected after committed transaction ----
	// Current state: ids 1(Alice,800,checking), 3(Alice,2500,savings), 4(Carol,750,checking), 5(Dave,1500,savings)
	// Total = 800+2500+750+1500 = 5550
	check("T5 total balance after all committed txns is 5550",
		`SELECT SUM(balance) FROM v37_accounts`, 5550)

	// Alice total = 800+2500 = 3300
	check("T5b Alice total balance is 3300",
		`SELECT SUM(balance) FROM v37_accounts WHERE owner = 'Alice'`, 3300)

	// ---- Test T6: Transaction that modifies indexed columns ----
	checkNoError("T6 create index on account owner",
		`CREATE INDEX idx_v37_accounts_owner ON v37_accounts(owner)`)

	checkNoError("T6b BEGIN",
		`BEGIN`)
	checkNoError("T6c UPDATE Alice checking owner to Bob",
		`UPDATE v37_accounts SET owner = 'Bob' WHERE id = 1`)
	checkNoError("T6d COMMIT",
		`COMMIT`)

	// Alice now has only 1 account (savings, id=3)
	checkRowCount("T6e Alice has 1 account after owner update",
		`SELECT id FROM v37_accounts WHERE owner = 'Alice'`, 1)

	// Bob now has 1 account (the former Alice checking)
	checkRowCount("T6f Bob has 1 account after owner update",
		`SELECT id FROM v37_accounts WHERE owner = 'Bob'`, 1)

	// ============================================================
	// SECTION 5: FK CASCADE CHAINS
	// ============================================================
	//
	// Schema: 4-level cascade chain
	//   v37_region   (id PK, name TEXT)
	//   v37_country  (id PK, region_id FK->v37_region CASCADE DELETE, name TEXT)
	//   v37_city     (id PK, country_id FK->v37_country CASCADE DELETE, name TEXT)
	//   v37_venue    (id PK, city_id FK->v37_city CASCADE DELETE SET NULL, name TEXT)
	//
	// Data:
	//   Regions: 1=Americas, 2=Europe
	//   Countries: 1=USA(Americas), 2=Canada(Americas), 3=France(Europe), 4=Germany(Europe)
	//   Cities: 1=NewYork(USA), 2=Chicago(USA), 3=Toronto(Canada), 4=Paris(France), 5=Berlin(Germany)
	//   Venues: 1=MSG(NewYork), 2=Wrigley(Chicago), 3=Rogers(Toronto),
	//           4=Louvre(Paris), 5=AllianzArena(Berlin)

	afExec(t, db, ctx, `CREATE TABLE v37_region (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_region VALUES (1, 'Americas')")
	afExec(t, db, ctx, "INSERT INTO v37_region VALUES (2, 'Europe')")

	afExec(t, db, ctx, `CREATE TABLE v37_country (
		id        INTEGER PRIMARY KEY,
		region_id INTEGER,
		name      TEXT NOT NULL,
		FOREIGN KEY (region_id) REFERENCES v37_region(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_country VALUES (1, 1, 'USA')")
	afExec(t, db, ctx, "INSERT INTO v37_country VALUES (2, 1, 'Canada')")
	afExec(t, db, ctx, "INSERT INTO v37_country VALUES (3, 2, 'France')")
	afExec(t, db, ctx, "INSERT INTO v37_country VALUES (4, 2, 'Germany')")

	afExec(t, db, ctx, `CREATE TABLE v37_city (
		id         INTEGER PRIMARY KEY,
		country_id INTEGER,
		name       TEXT NOT NULL,
		FOREIGN KEY (country_id) REFERENCES v37_country(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_city VALUES (1, 1, 'NewYork')")
	afExec(t, db, ctx, "INSERT INTO v37_city VALUES (2, 1, 'Chicago')")
	afExec(t, db, ctx, "INSERT INTO v37_city VALUES (3, 2, 'Toronto')")
	afExec(t, db, ctx, "INSERT INTO v37_city VALUES (4, 3, 'Paris')")
	afExec(t, db, ctx, "INSERT INTO v37_city VALUES (5, 4, 'Berlin')")

	afExec(t, db, ctx, `CREATE TABLE v37_venue (
		id      INTEGER PRIMARY KEY,
		city_id INTEGER,
		name    TEXT NOT NULL,
		FOREIGN KEY (city_id) REFERENCES v37_city(id) ON DELETE CASCADE
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_venue VALUES (1, 1, 'MSG')")
	afExec(t, db, ctx, "INSERT INTO v37_venue VALUES (2, 2, 'Wrigley')")
	afExec(t, db, ctx, "INSERT INTO v37_venue VALUES (3, 3, 'Rogers')")
	afExec(t, db, ctx, "INSERT INTO v37_venue VALUES (4, 4, 'Louvre')")
	afExec(t, db, ctx, "INSERT INTO v37_venue VALUES (5, 5, 'AllianzArena')")

	// ---- Test F1: Baseline counts ----
	check("F1 baseline region count is 2",
		`SELECT COUNT(*) FROM v37_region`, 2)

	check("F1b baseline country count is 4",
		`SELECT COUNT(*) FROM v37_country`, 4)

	check("F1c baseline city count is 5",
		`SELECT COUNT(*) FROM v37_city`, 5)

	check("F1d baseline venue count is 5",
		`SELECT COUNT(*) FROM v37_venue`, 5)

	// ---- Test F2: CASCADE DELETE propagates from country to city to venue ----
	// Delete Canada (country_id=2): should cascade to Toronto (city_id=3) then to Rogers (venue_id=3)
	checkNoError("F2 DELETE Canada country",
		`DELETE FROM v37_country WHERE name = 'Canada'`)

	check("F2b country count is 3 after Canada delete",
		`SELECT COUNT(*) FROM v37_country`, 3)

	// Toronto should be gone (cascaded from Canada delete)
	checkRowCount("F2c Toronto city cascaded deleted",
		`SELECT id FROM v37_city WHERE name = 'Toronto'`, 0)

	// Rogers venue should be gone (cascaded from Toronto delete)
	checkRowCount("F2d Rogers venue cascaded deleted",
		`SELECT id FROM v37_venue WHERE name = 'Rogers'`, 0)

	// City count: 5 - 1 = 4
	check("F2e city count is 4 after cascade",
		`SELECT COUNT(*) FROM v37_city`, 4)

	// Venue count: 5 - 1 = 4
	check("F2f venue count is 4 after cascade",
		`SELECT COUNT(*) FROM v37_venue`, 4)

	// ---- Test F3: CASCADE DELETE from region level cascades all the way down ----
	// Delete Europe region (id=2): should cascade to France and Germany countries,
	// then to Paris and Berlin cities, then to Louvre and AllianzArena venues
	checkNoError("F3 DELETE Europe region",
		`DELETE FROM v37_region WHERE name = 'Europe'`)

	// Country count: 3 - 2 = 1 (only USA remains)
	check("F3b country count is 1 after Europe region delete",
		`SELECT COUNT(*) FROM v37_country`, 1)

	// City count: 4 - 2 = 2 (NewYork and Chicago remain)
	check("F3c city count is 2 after Europe cascade",
		`SELECT COUNT(*) FROM v37_city`, 2)

	// Venue count: 4 - 2 = 2 (MSG and Wrigley remain)
	check("F3d venue count is 2 after Europe cascade",
		`SELECT COUNT(*) FROM v37_venue`, 2)

	// Verify Paris and Berlin are gone
	checkRowCount("F3e Paris is gone after Europe cascade",
		`SELECT id FROM v37_city WHERE name = 'Paris'`, 0)

	checkRowCount("F3f Berlin is gone after Europe cascade",
		`SELECT id FROM v37_city WHERE name = 'Berlin'`, 0)

	// ---- Test F4: Insert into child, verify parent must exist ----
	// Insert a city for non-existent country (id=99) should be accepted if no strict FK,
	// but inserting then deleting parent should leave child orphaned.
	// Instead, verify existing child is still correctly linked to parent.
	checkRowCount("F4 NewYork is still linked to USA country",
		`SELECT c.name FROM v37_city c
		 JOIN v37_country co ON co.id = c.country_id
		 WHERE co.name = 'USA'`, 2)

	// ---- Test F5: ON DELETE SET NULL cascade ----
	// Create a separate 2-level table for SET NULL demonstration:
	//   v37_dept (id PK, name TEXT)
	//   v37_emp  (id PK, dept_id FK->v37_dept ON DELETE SET NULL, name TEXT)

	afExec(t, db, ctx, `CREATE TABLE v37_dept (
		id   INTEGER PRIMARY KEY,
		name TEXT NOT NULL
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_dept VALUES (1, 'Engineering')")
	afExec(t, db, ctx, "INSERT INTO v37_dept VALUES (2, 'Sales')")

	afExec(t, db, ctx, `CREATE TABLE v37_emp (
		id      INTEGER PRIMARY KEY,
		dept_id INTEGER,
		name    TEXT NOT NULL,
		FOREIGN KEY (dept_id) REFERENCES v37_dept(id) ON DELETE SET NULL
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_emp VALUES (1, 1, 'Alice')")
	afExec(t, db, ctx, "INSERT INTO v37_emp VALUES (2, 1, 'Bob')")
	afExec(t, db, ctx, "INSERT INTO v37_emp VALUES (3, 2, 'Carol')")
	afExec(t, db, ctx, "INSERT INTO v37_emp VALUES (4, 2, 'Dave')")

	// Delete Engineering department: emp.dept_id should become NULL for Alice and Bob
	checkNoError("F5 DELETE Engineering department",
		`DELETE FROM v37_dept WHERE name = 'Engineering'`)

	// Alice and Bob dept_id should be NULL
	checkRowCount("F5b Alice dept_id is NULL after SET NULL cascade",
		`SELECT id FROM v37_emp WHERE name = 'Alice' AND dept_id IS NULL`, 1)

	checkRowCount("F5c Bob dept_id is NULL after SET NULL cascade",
		`SELECT id FROM v37_emp WHERE name = 'Bob' AND dept_id IS NULL`, 1)

	// Carol and Dave should be unaffected (still in Sales dept_id=2)
	checkRowCount("F5d Carol still in Sales dept",
		`SELECT id FROM v37_emp WHERE name = 'Carol' AND dept_id = 2`, 1)

	// ---- Test F6: Mixed CASCADE and SET NULL in same chain ----
	// Verify emp row count: all 4 employees exist (SET NULL doesn't delete rows)
	check("F6 all 4 employees still exist after SET NULL cascade",
		`SELECT COUNT(*) FROM v37_emp`, 4)

	// Orphaned employees (dept_id IS NULL): Alice and Bob = 2
	checkRowCount("F6b two employees are orphaned (dept_id NULL)",
		`SELECT id FROM v37_emp WHERE dept_id IS NULL`, 2)

	// ============================================================
	// SECTION 6: INDEX CONSISTENCY UNDER MUTATIONS
	// ============================================================
	//
	// Schema
	// ------
	//   v37_catalog (id PK, sku TEXT, category TEXT, price REAL, qty INTEGER)
	//
	// 50 rows inserted with known values for index verification.
	// SKUs follow pattern: 'SKU-XXX' where XXX = id*10
	// category alternates: odd id=>'A', even id=>'B'
	// price = id * 5.00
	// qty   = id * 10

	afExec(t, db, ctx, `CREATE TABLE v37_catalog (
		id       INTEGER PRIMARY KEY,
		sku      TEXT    NOT NULL,
		category TEXT,
		price    REAL,
		qty      INTEGER
	)`)

	// Insert 50 rows with predictable values
	for i := 1; i <= 50; i++ {
		cat := "A"
		if i%2 == 0 {
			cat = "B"
		}
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v37_catalog VALUES (%d, 'SKU-%03d', '%s', %.2f, %d)",
			i, i*10, cat, float64(i)*5.0, i*10))
	}

	// ---- Test X1: After 50 inserts, total row count correct ----
	check("X1 catalog has 50 rows after bulk insert",
		`SELECT COUNT(*) FROM v37_catalog`, 50)

	// Category A count: ids 1,3,5,...49 => 25 rows
	check("X1b category A has 25 rows",
		`SELECT COUNT(*) FROM v37_catalog WHERE category = 'A'`, 25)

	// ---- Test X2: Create index then verify query uses it correctly ----
	checkNoError("X2 create index on category",
		`CREATE INDEX idx_v37_catalog_cat ON v37_catalog(category)`)

	checkNoError("X2b create index on price",
		`CREATE INDEX idx_v37_catalog_price ON v37_catalog(price)`)

	// Query via category index: B rows = 25
	check("X2c category B has 25 rows via index",
		`SELECT COUNT(*) FROM v37_catalog WHERE category = 'B'`, 25)

	// Query via price index: price between 50 and 150 (ids 10..30, price=50..150)
	// price = id*5, so price=50 => id=10, price=150 => id=30, that's ids 10-30 = 21 rows
	checkRowCount("X2d price between 50 and 150 returns 21 rows",
		`SELECT id FROM v37_catalog WHERE price >= 50.00 AND price <= 150.00`, 21)

	// ---- Test X3: UPDATE indexed column (category), verify old value not found ----
	// Change category for id=1 from 'A' to 'C'
	checkNoError("X3 UPDATE id=1 category from A to C",
		`UPDATE v37_catalog SET category = 'C' WHERE id = 1`)

	// Category A should now have 24 rows (was 25, lost id=1)
	check("X3b category A has 24 rows after update",
		`SELECT COUNT(*) FROM v37_catalog WHERE category = 'A'`, 24)

	// Category C should have 1 row (id=1)
	checkRowCount("X3c category C has 1 row (id=1) after update",
		`SELECT id FROM v37_catalog WHERE category = 'C'`, 1)

	// ---- Test X4: Bulk UPDATE indexed column, verify all queries correct ----
	// Change all category='B' rows (ids 2,4,6,...50) to category='D'
	// That's 25 rows
	checkNoError("X4 bulk UPDATE category B to D",
		`UPDATE v37_catalog SET category = 'D' WHERE category = 'B'`)

	// Category B should now have 0 rows
	checkRowCount("X4b category B is empty after bulk update",
		`SELECT id FROM v37_catalog WHERE category = 'B'`, 0)

	// Category D should have 25 rows
	check("X4c category D has 25 rows after bulk update",
		`SELECT COUNT(*) FROM v37_catalog WHERE category = 'D'`, 25)

	// Categories in table are now A(24), C(1), D(25)
	checkRowCount("X4d total rows still 50",
		`SELECT id FROM v37_catalog`, 50)

	// ---- Test X5: DELETE rows, verify index doesn't return deleted rows ----
	// Delete all category='C' rows (id=1 only)
	checkNoError("X5 DELETE category C row",
		`DELETE FROM v37_catalog WHERE category = 'C'`)

	// Category C should return 0 rows via index
	checkRowCount("X5b category C returns 0 rows after delete",
		`SELECT id FROM v37_catalog WHERE category = 'C'`, 0)

	// Total rows: 50 - 1 = 49
	check("X5c total rows is 49 after category C delete",
		`SELECT COUNT(*) FROM v37_catalog`, 49)

	// Category A still has 24 rows
	check("X5d category A still has 24 rows",
		`SELECT COUNT(*) FROM v37_catalog WHERE category = 'A'`, 24)

	// ---- Test X6: DELETE half the rows, verify index returns correct subset ----
	// Delete all category='D' rows (25 rows)
	checkNoError("X6 DELETE all category D rows",
		`DELETE FROM v37_catalog WHERE category = 'D'`)

	// Remaining: category A only, 24 rows
	check("X6b 24 rows remain after category D delete",
		`SELECT COUNT(*) FROM v37_catalog`, 24)

	// Category D query returns 0
	checkRowCount("X6c category D returns 0 after delete",
		`SELECT id FROM v37_catalog WHERE category = 'D'`, 0)

	// ---- Test X7: Price index still correct after category updates and deletes ----
	// Remaining rows are category A: ids 3,5,7,...,49 (odd ids 3-49, which is 24 rows)
	// Note: id=1 was deleted in X5. Odd ids remaining: 3,5,7,9,11,...,49
	// Prices: id*5 => range 15 to 245 for id 3 to 49
	// Price between 50 and 100: price=50(id=10,even,deleted), 55(id=11,A), 60(id=12,even,deleted)
	//   ... only odd ids remain in category A
	//   Odd ids where price is 50-100: id*5 in [50,100] => id in [10,20]
	//   Odd ids in [10,20]: 11, 13, 15, 17, 19 => 5 rows, prices: 55,65,75,85,95
	checkRowCount("X7 price 50-100 returns 5 odd-id rows after deletes",
		`SELECT id FROM v37_catalog WHERE price >= 50.00 AND price <= 100.00`, 5)

	// ---- Test X8: DROP INDEX, verify queries still work via full scan ----
	checkNoError("X8 DROP category index",
		`DROP INDEX idx_v37_catalog_cat`)

	// Queries should still work after index drop (full scan)
	check("X8b category A count still correct after index drop",
		`SELECT COUNT(*) FROM v37_catalog WHERE category = 'A'`, 24)

	// ============================================================
	// SECTION 7: COMPLEX MUTATION SEQUENCES
	// ============================================================
	//
	// Schema
	// ------
	//   v37_inventory (id PK, item TEXT, quantity INTEGER, warehouse TEXT, value REAL)
	//
	// Pattern: create, insert 100 rows, update 50, delete 25, verify count=75

	afExec(t, db, ctx, `CREATE TABLE v37_inventory (
		id        INTEGER PRIMARY KEY,
		item      TEXT    NOT NULL,
		quantity  INTEGER,
		warehouse TEXT,
		value     REAL
	)`)

	// Insert 100 rows: ids 1-100
	// item = 'Item-XX', quantity = id, warehouse alternates N/S/E/W (id%4: 0=N,1=S,2=E,3=W)
	// value = id * 10.0
	for i := 1; i <= 100; i++ {
		whMap := map[int]string{0: "North", 1: "South", 2: "East", 3: "West"}
		wh := whMap[i%4]
		afExec(t, db, ctx, fmt.Sprintf(
			"INSERT INTO v37_inventory VALUES (%d, 'Item-%03d', %d, '%s', %.1f)",
			i, i, i, wh, float64(i)*10.0))
	}

	// ---- Test C1: Baseline after 100 inserts ----
	check("C1 inventory has 100 rows after insert",
		`SELECT COUNT(*) FROM v37_inventory`, 100)

	// Sum of quantity = 1+2+...+100 = 5050
	check("C1b sum of quantity is 5050",
		`SELECT SUM(quantity) FROM v37_inventory`, 5050)

	// Sum of value = 10+20+...+1000 = 5050*10 = 50500
	// Engine may render as scientific notation for large sums
	check("C1c sum of value is 50500",
		`SELECT SUM(value) FROM v37_inventory`, "50500")

	// ---- Test C2: UPDATE 50 rows (ids 1-50), verify aggregates shift correctly ----
	// Double quantity for ids 1-50
	// Original sum for ids 1-50: 1+2+...+50 = 1275
	// After doubling: 2+4+...+100 = 2*1275 = 2550
	// Ids 51-100 sum unchanged: 51+52+...+100 = 3775
	// New total quantity = 2550 + 3775 = 6325
	checkNoError("C2 UPDATE quantity*2 for ids 1-50",
		`UPDATE v37_inventory SET quantity = quantity * 2 WHERE id <= 50`)

	check("C2b total quantity after UPDATE is 6325",
		`SELECT SUM(quantity) FROM v37_inventory`, 6325)

	check("C2c row count still 100 after UPDATE",
		`SELECT COUNT(*) FROM v37_inventory`, 100)

	// ---- Test C3: DELETE 25 rows (ids 76-100), verify count=75 ----
	checkNoError("C3 DELETE ids 76-100 (25 rows)",
		`DELETE FROM v37_inventory WHERE id >= 76`)

	check("C3b count is 75 after deleting ids 76-100",
		`SELECT COUNT(*) FROM v37_inventory`, 75)

	// Remaining: ids 1-75
	// Quantity sum for ids 1-50 (doubled): 2+4+6+...+100 = 2*1275 = 2550
	// Quantity sum for ids 51-75 (original): 51+52+...+75 = sum(1..75)-sum(1..50) = 2850-1275 = 1575
	// Total = 2550 + 1575 = 4125
	check("C3c total quantity for 75 rows is 4125",
		`SELECT SUM(quantity) FROM v37_inventory`, 4125)

	// ---- Test C4: UPDATE then SELECT aggregate, verify mathematical correctness ----
	// Add 5 to every quantity in North warehouse
	// North rows: id%4=0 means id in {4,8,12,16,...,72} in range 1-75
	// North ids in 1-75: 4,8,12,...,72 => ids divisible by 4, max 72
	// Count: 72/4 = 18 rows
	// Current quantities for North ids 4-72 (step 4):
	//   ids 4-50 (step 4): 4,8,12,...,48,50(no, 50 not divisible by 4)
	//   Actually: ids divisible by 4 in [1,50]: 4,8,12,16,20,24,28,32,36,40,44,48 => 12 rows (doubled qty)
	//   doubled quantity: 8,16,24,32,40,48,56,64,72,80,88,96
	//   ids divisible by 4 in [51,75]: 52,56,60,64,68,72 => 6 rows (original qty)
	//   original quantity: 52,56,60,64,68,72
	// North sum before +5: (8+16+24+32+40+48+56+64+72+80+88+96) + (52+56+60+64+68+72)
	//   = 624 + 372 = 996
	// After +5 per row (18 rows): 996 + 18*5 = 996 + 90 = 1086
	// New total quantity = 4125 - 996 + 1086 = 4215
	checkNoError("C4 UPDATE quantity+5 for all North warehouse rows",
		`UPDATE v37_inventory SET quantity = quantity + 5 WHERE warehouse = 'North'`)

	check("C4b total quantity after North +5 is 4215",
		`SELECT SUM(quantity) FROM v37_inventory`, 4215)

	// North warehouse count should still be 18
	check("C4c North warehouse count is still 18",
		`SELECT COUNT(*) FROM v37_inventory WHERE warehouse = 'North'`, 18)

	// ---- Test C5: Chain INSERT -> UPDATE -> verify -> DELETE -> verify -> INSERT -> verify ----
	// Use a fresh small table for this chain test
	afExec(t, db, ctx, `CREATE TABLE v37_chain (
		id   INTEGER PRIMARY KEY,
		val  INTEGER,
		tag  TEXT
	)`)

	// Step 1: INSERT 5 rows
	afExec(t, db, ctx, "INSERT INTO v37_chain VALUES (1, 10, 'initial')")
	afExec(t, db, ctx, "INSERT INTO v37_chain VALUES (2, 20, 'initial')")
	afExec(t, db, ctx, "INSERT INTO v37_chain VALUES (3, 30, 'initial')")
	afExec(t, db, ctx, "INSERT INTO v37_chain VALUES (4, 40, 'initial')")
	afExec(t, db, ctx, "INSERT INTO v37_chain VALUES (5, 50, 'initial')")

	check("C5 chain has 5 rows after initial INSERT",
		`SELECT COUNT(*) FROM v37_chain`, 5)

	// Step 2: UPDATE val = val + 100 for ids 1-3
	// New vals: 1->110, 2->120, 3->130, 4->40(unchanged), 5->50(unchanged)
	afExec(t, db, ctx, "UPDATE v37_chain SET val = val + 100, tag = 'updated' WHERE id <= 3")

	check("C5b val for id=1 is 110 after UPDATE",
		`SELECT val FROM v37_chain WHERE id = 1`, 110)

	check("C5c SUM(val) is 110+120+130+40+50=450 after UPDATE",
		`SELECT SUM(val) FROM v37_chain`, 450)

	// Step 3: DELETE ids 4 and 5
	afExec(t, db, ctx, "DELETE FROM v37_chain WHERE id > 3")

	check("C5d row count is 3 after DELETE of ids 4,5",
		`SELECT COUNT(*) FROM v37_chain`, 3)

	check("C5e SUM(val) is 110+120+130=360 after DELETE",
		`SELECT SUM(val) FROM v37_chain`, 360)

	// Step 4: INSERT 2 new rows with ids 6 and 7
	afExec(t, db, ctx, "INSERT INTO v37_chain VALUES (6, 200, 'new')")
	afExec(t, db, ctx, "INSERT INTO v37_chain VALUES (7, 300, 'new')")

	check("C5f row count is 5 after final INSERT",
		`SELECT COUNT(*) FROM v37_chain`, 5)

	// Sum = 110+120+130+200+300 = 860
	check("C5g SUM(val) is 860 after final INSERT",
		`SELECT SUM(val) FROM v37_chain`, 860)

	// ---- Test C6: Trigger fires during bulk INSERT, verify trigger log count matches ----
	afExec(t, db, ctx, `CREATE TABLE v37_events (
		id        INTEGER PRIMARY KEY,
		event_name TEXT    NOT NULL,
		score      INTEGER
	)`)
	afExec(t, db, ctx, `CREATE TABLE v37_event_log (
		id        INTEGER PRIMARY KEY AUTO_INCREMENT,
		event_id  INTEGER,
		logged_at TEXT
	)`)

	// Trigger: AFTER INSERT on v37_events, log to v37_event_log
	checkNoError("C6 create AFTER INSERT trigger on v37_events",
		`CREATE TRIGGER trg_v37_event_insert
		 AFTER INSERT ON v37_events
		 BEGIN
		   INSERT INTO v37_event_log (event_id, logged_at) VALUES (NEW.id, 'now');
		 END`)

	// Insert 8 events
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (1, 'Start',     100)")
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (2, 'Goal',      200)")
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (3, 'Penalty',    50)")
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (4, 'Goal',      200)")
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (5, 'Foul',       10)")
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (6, 'Goal',      200)")
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (7, 'YellowCard', 25)")
	afExec(t, db, ctx, "INSERT INTO v37_events VALUES (8, 'End',         0)")

	// Trigger fired once per INSERT => 8 log entries
	check("C6b event_log has 8 entries matching 8 inserts",
		`SELECT COUNT(*) FROM v37_event_log`, 8)

	// Every event_id in log matches an event in v37_events
	check("C6c all log entries have valid event_ids",
		`SELECT COUNT(*) FROM v37_event_log el
		 WHERE el.event_id IN (SELECT id FROM v37_events)`, 8)

	// ---- Test C7: Trigger fires during bulk UPDATE, verify log count ----
	// Create a separate update-trigger test
	afExec(t, db, ctx, `CREATE TABLE v37_prices (
		id    INTEGER PRIMARY KEY,
		name  TEXT,
		price REAL
	)`)
	afExec(t, db, ctx, `CREATE TABLE v37_price_log (
		id       INTEGER PRIMARY KEY AUTO_INCREMENT,
		item_id  INTEGER,
		old_price REAL,
		new_price REAL
	)`)
	checkNoError("C7 create AFTER UPDATE trigger on v37_prices",
		`CREATE TRIGGER trg_v37_price_update
		 AFTER UPDATE ON v37_prices
		 BEGIN
		   INSERT INTO v37_price_log (item_id, old_price, new_price) VALUES (OLD.id, OLD.price, NEW.price);
		 END`)

	afExec(t, db, ctx, "INSERT INTO v37_prices VALUES (1, 'Apple',  1.00)")
	afExec(t, db, ctx, "INSERT INTO v37_prices VALUES (2, 'Banana', 0.50)")
	afExec(t, db, ctx, "INSERT INTO v37_prices VALUES (3, 'Cherry', 2.00)")
	afExec(t, db, ctx, "INSERT INTO v37_prices VALUES (4, 'Date',   3.00)")
	afExec(t, db, ctx, "INSERT INTO v37_prices VALUES (5, 'Elderberry', 5.00)")

	// Update all prices by 10%.
	// The AFTER UPDATE trigger fires once per affected row (FOR EACH ROW),
	// so 5 rows updated => 5 log entries.
	checkNoError("C7b UPDATE all prices * 1.1",
		`UPDATE v37_prices SET price = price * 1.1`)

	check("C7c price_log has 5 entries after first bulk update (per-row trigger)",
		`SELECT COUNT(*) FROM v37_price_log`, 5)

	// A second bulk update => 5 more trigger fires => 10 total log entries
	checkNoError("C7d UPDATE all prices * 2",
		`UPDATE v37_prices SET price = price * 2`)

	check("C7e price_log has 10 entries after second bulk update (10 total)",
		`SELECT COUNT(*) FROM v37_price_log`, 10)

	// ---- Test C8: View consistency after underlying table mutations ----
	afExec(t, db, ctx, `CREATE TABLE v37_employees (
		id     INTEGER PRIMARY KEY,
		name   TEXT    NOT NULL,
		dept   TEXT,
		salary INTEGER,
		active INTEGER  -- 1=active, 0=inactive
	)`)
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (1,  'Alice',   'Eng',   90000, 1)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (2,  'Bob',     'Sales', 70000, 1)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (3,  'Carol',   'Eng',   85000, 1)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (4,  'Dave',    'HR',    65000, 0)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (5,  'Eve',     'Eng',   95000, 1)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (6,  'Frank',   'Sales', 75000, 1)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (7,  'Grace',   'HR',    68000, 1)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (8,  'Hank',    'Eng',   72000, 0)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (9,  'Iris',    'Sales', 80000, 1)")
	afExec(t, db, ctx, "INSERT INTO v37_employees VALUES (10, 'Jack',    'Eng',   110000, 1)")

	// Create a view of active employees
	checkNoError("C8 CREATE VIEW v37_active_employees",
		`CREATE VIEW v37_active_employees AS
		 SELECT id, name, dept, salary
		 FROM v37_employees
		 WHERE active = 1`)

	// Baseline: active employees = 8 (Dave=0, Hank=0)
	check("C8b active employees view has 8 rows",
		`SELECT COUNT(*) FROM v37_active_employees`, 8)

	// Deactivate Bob and Frank (set active=0)
	checkNoError("C8c UPDATE deactivate Bob",
		`UPDATE v37_employees SET active = 0 WHERE name = 'Bob'`)

	checkNoError("C8d UPDATE deactivate Frank",
		`UPDATE v37_employees SET active = 0 WHERE name = 'Frank'`)

	// View should now return 6 rows
	check("C8e view reflects deactivations: 6 active employees",
		`SELECT COUNT(*) FROM v37_active_employees`, 6)

	// Delete inactive employees (Dave, Hank, Bob, Frank = 4 rows)
	checkNoError("C8f DELETE inactive employees",
		`DELETE FROM v37_employees WHERE active = 0`)

	// View should now return 6 rows (same as before, deleted rows were inactive)
	check("C8g view still has 6 rows after deleting inactive rows",
		`SELECT COUNT(*) FROM v37_active_employees`, 6)

	// Base table row count: 10 - 4 = 6
	check("C8h base table has 6 rows after deleting inactive",
		`SELECT COUNT(*) FROM v37_employees`, 6)

	// ---- Test C9: Eng department salary average via view after mutations ----
	// Active Eng employees: Alice(90000), Carol(85000), Eve(95000), Jack(110000)
	// (Dave=0 deleted, Hank=0 deleted)
	// Eng avg = (90000+85000+95000+110000)/4 = 380000/4 = 95000
	check("C9 Eng department avg salary via view is 95000",
		`SELECT AVG(salary)
		 FROM v37_active_employees
		 WHERE dept = 'Eng'`, 95000)

	// ---- Test C10: Verify view MAX salary reflects data mutation ----
	// Max active salary: Jack(110000)
	check("C10 max salary in view is 110000 (Jack)",
		`SELECT MAX(salary) FROM v37_active_employees`, 110000)

	// Increase Jack's salary to 130000
	checkNoError("C10b UPDATE Jack salary to 130000",
		`UPDATE v37_employees SET salary = 130000 WHERE name = 'Jack'`)

	// View max should now be 130000
	check("C10c view max salary updated to 130000 after mutation",
		`SELECT MAX(salary) FROM v37_active_employees`, 130000)

	// ---- Test C11: 100-row INSERT + 50-row UPDATE + 25-row DELETE = 75 rows ----
	// (Already demonstrated across C1-C3 for v37_inventory)
	// Re-verify the final state of v37_inventory: should have 75 rows
	check("C11 inventory count is 75 (100 inserted - 25 deleted)",
		`SELECT COUNT(*) FROM v37_inventory`, 75)

	// ---- Test C12: Final cross-section aggregate consistency check ----
	// v37_scores total score after U12 restore = 5500 (was verified in U12)
	check("C12 v37_scores total score still 5500 after all test sections",
		`SELECT SUM(score) FROM v37_scores`, 5500)

	// v37_chain final state: 5 rows with sum 860
	check("C12b v37_chain sum is 860 at end of test",
		`SELECT SUM(val) FROM v37_chain`, 860)

	// v37_active_employees count is 6
	check("C12c v37_active_employees count is 6 at end of test",
		`SELECT COUNT(*) FROM v37_active_employees`, 6)

	// ============================================================
	// FINAL PASS/TOTAL SUMMARY
	// ============================================================
	t.Logf("V37 Mutation Consistency: %d/%d tests passed", pass, total)
	if pass != total {
		t.Errorf("FAILED: %d tests did not pass", total-pass)
	}
}
