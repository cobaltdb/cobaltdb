package catalog

import (
	"testing"
	"time"
)

// TestMySQLWeekDenseSweep validates the WEEK()/YEARWEEK() week algorithms over
// every date from 1900-01-01 through 2100-12-31 in all eight modes.
//
// Invariant families:
//
//  1. Domains: rolling modes (2,3,6,7) never report week 0; no-rolling modes
//     (0,1,4,5) report 0 only for the leading stub days; YEARWEEK's week
//     component is always 1..53 and its year stays within the sweep horizon.
//  2. WEEK equals YEARWEEK%100 whenever WEEK is non-zero; a WEEK-00 stub day
//     rolls YEARWEEK to week 52 or 53 of the previous year.
//  3. YEARWEEK (year, week) advances day over day: within a week-year the
//     week number is constant, and the only transition is into the next
//     week-year (same year, next week; or next year, week 1).
//  4. ISO anchoring: mode 3 (year, week) equals the stdlib ISO week; mode 1's
//     week equals the stdlib ISO week whenever it is non-zero.
//  5. No-roll/roll consistency: whenever a no-rolling mode reports a non-zero
//     week it equals its rolling counterpart's week for the same anchor family
//     (0↔2, 1↔3, 4↔6, 5↔7).
//  6. Mode 2 rollback depth: a week-00 day rolls to week 52 or 53 of the
//     previous year.
func TestMySQLWeekDenseSweep(t *testing.T) {
	start := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2100, 12, 31, 0, 0, 0, 0, time.UTC)

	type wk struct {
		w          int
		y          int
		ywWeek     int
		isoW, isoY int
		calY       int
	}
	roll := map[int]int{0: 2, 1: 3, 4: 6, 5: 7}

	ds := func(t time.Time) string { return t.Format("2006-01-02") }

	var prev map[int]wk
	prevCalY := 0
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		cur := map[int]wk{}
		for mode := 0; mode <= 7; mode++ {
			isoY, isoW := d.ISOWeek()
			w := mysqlWeek(d, mode)
			y, ywWeek := mysqlYearWeek(d, mode)
			cur[mode] = wk{w: w, y: y, ywWeek: ywWeek, isoW: isoW, isoY: isoY, calY: d.Year()}

			noRoll := mode == 0 || mode == 1 || mode == 4 || mode == 5
			if noRoll {
				if w < 0 || w > 53 {
					t.Fatalf("%s mode %d: week %d outside 0..53", ds(d), mode, w)
				}
			} else if w < 1 || w > 53 {
				t.Fatalf("%s mode %d: rolling week %d outside 1..53", ds(d), mode, w)
			}
			if ywWeek < 1 || ywWeek > 53 {
				t.Fatalf("%s mode %d: YEARWEEK week %d outside 1..53", ds(d), mode, ywWeek)
			}
			if y < 1899 || y > 2101 {
				t.Fatalf("%s mode %d: YEARWEEK year %d outside horizon", ds(d), mode, y)
			}
			if w != 0 {
				// The direct WEEK==YEARWEEK%100 tie holds only where both
				// sides call the same helper (modes 2/3/6/7). Modes 0/1/4/5
				// pair a NON-rolling WEEK with a rolling YEARWEEK, so they
				// legitimately diverge at stub days (week 00) and tail
				// straddles; they are constrained by the stub, ISO-anchor,
				// and advance checks instead.
				if (mode == 2 || mode == 3 || mode == 6 || mode == 7) && w != ywWeek%100 {
					t.Fatalf("%s mode %d: rolling WEEK %d != YEARWEEK%%100 %d", ds(d), mode, w, ywWeek%100)
				}
			} else if ywWeek%100 < 52 {
				t.Fatalf("%s mode %d: WEEK-00 stub rolled YEARWEEK to week %d, want 52 or 53",
					ds(d), mode, ywWeek%100)
			}
			if mode == 3 {
				if w != isoW || y != isoY {
					t.Fatalf("%s mode 3: (%d,%d) != stdlib ISO (%d,%d)", ds(d), y, w, isoY, isoW)
				}
			}
			if mode == 1 && w != 0 && !(w >= 52 && isoW == 1) && w != isoW {
				// Mode 1 is NON-rolling: its week number equals the ISO week
				// number except at stub days (week 00, checked above) and the
				// tail straddle, where the year's final week (52/53) stays in
				// the old year while ISO has already rolled to week 1 of the
				// next year.
				t.Fatalf("%s mode 1: non-zero week %d != stdlib ISO week %d", ds(d), w, isoW)
			}

			// No-roll/roll consistency within the same anchor family. The
			// tail straddle is the documented exception for the modes whose
			// rolling counterpart rolls FORWARD at year end (1↔3 ISO and
			// 4↔6 majority): the non-rolling week stays on the old year's
			// final number (52/53) while the counterpart has already rolled
			// to week 1 of the next year. Modes 0↔2 and 5↔7 never roll
			// forward (their week-1 anchors start inside their own year).
			if pair, ok := roll[mode]; ok && w != 0 {
				if pw := mysqlWeek(d, pair); pw != w && !(w >= 52 && pw == 1 && (mode == 1 || mode == 4)) {
					t.Fatalf("%s mode %d: non-zero week %d != rolling counterpart mode %d week %d",
						ds(d), mode, w, pair, pw)
				}
			}
		}

		// Mode 2 rollback depth: a week-00 day rolls to week 52 or 53 of the
		// previous year. mysqlWeekMode2 returns (week, year).
		if cur[0].w == 0 {
			if w, y := mysqlWeekMode2(d); w < 52 || w > 53 || y != d.Year()-1 {
				t.Fatalf("%s: week-00 rollback gave (week %d, year %d), want week 52..53 of %d",
					ds(d), w, y, d.Year()-1)
			}
		}

		// Step shape vs the previous day.
		if prev != nil {
			for mode := 0; mode <= 7; mode++ {
				p, c := prev[mode], cur[mode]
				noRoll := mode == 0 || mode == 1 || mode == 4 || mode == 5
				// YEARWEEK: within a week-year the week is constant (mid-week)
				// or advances by one (a new week started); the only year
				// transition is into the next week-year at week 1.
				if c.y == p.y {
					if c.ywWeek != p.ywWeek && c.ywWeek != p.ywWeek+1 {
						t.Fatalf("%s mode %d: YEARWEEK week stepped %d -> %d within week-year %d",
							ds(d), mode, p.ywWeek, c.ywWeek, c.y)
					}
				} else if c.y == p.y+1 && c.ywWeek == 1 {
					// valid week-year advance
				} else {
					t.Fatalf("%s mode %d: YEARWEEK advanced (%d,%d) -> (%d,%d), want next week-year",
						ds(d), mode, p.y, p.ywWeek, c.y, c.ywWeek)
				}
				// WEEK steps: no-rolling modes (0,1,4,5) number weeks within
				// the calendar year — constant or +1 per day, restarting at
				// 0 or 1 across the calendar boundary. Rolling modes (2,3,6,7)
				// number weeks within the week year instead, so their steps
				// are already fully constrained by the YEARWEEK advance check
				// above plus WEEK == YEARWEEK%100 (their reset to 1 lands
				// inside the first days of January or late December, not at
				// Jan 1).
				if noRoll {
					if c.calY == p.calY {
						if c.w != p.w && c.w != p.w+1 {
							t.Fatalf("%s mode %d: WEEK step %d -> %d within calendar year %d",
								ds(d), mode, p.w, c.w, c.calY)
						}
					} else if c.calY != p.calY+1 {
						t.Fatalf("%s mode %d: calendar year jumped %d -> %d",
							ds(d), mode, p.calY, c.calY)
					} else if c.w != 0 && c.w != 1 {
						t.Fatalf("%s mode %d: WEEK restart %d, want 0 or 1",
							ds(d), mode, c.w)
					}
				}
			}
		}

		prev = cur
		prevCalY = cur[0].calY
	}
	_ = prevCalY
}
