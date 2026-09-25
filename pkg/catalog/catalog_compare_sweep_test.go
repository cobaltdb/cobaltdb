package catalog

import (
	"testing"
)

// TestCatalogCompareValuesWeakOrder sweeps catalogCompareValues — the
// comparator behind every ORDER BY sort — over a mixed-type corpus and
// asserts the weak-order axioms a sort requires: antisymmetry and the three
// transitivity laws. A comparator that treats the string "NaN" (parsed by
// toFloat64 via strconv.ParseFloat) as numerically equal to EVERY number
// violates transitivity and makes ORDER BY output order-dependent.
func TestCatalogCompareValuesWeakOrder(t *testing.T) {
	corpus := []interface{}{
		nil,
		int64(5), int64(-3), int64(0),
		float64(2.5), float64(-7.25),
		true, false,
		"5", "10", "NaN", "Inf", "-Inf", "apple", "", "Z",
	}

	cmp := catalogCompareValues

	// Antisymmetry: cmp(a,b) == -cmp(b,a) for every ordered pair.
	for _, a := range corpus {
		for _, b := range corpus {
			ab, ba := cmp(a, b), cmp(b, a)
			if ab != -ba {
				t.Fatalf("antisymmetry violated: cmp(%#v, %#v) = %d but cmp(%#v, %#v) = %d",
					a, b, ab, b, a, ba)
			}
		}
	}

	// Transitivity of equivalence: a ≡ b ∧ b ≡ c ⟹ a ≡ c.
	for _, a := range corpus {
		for _, b := range corpus {
			if cmp(a, b) != 0 {
				continue
			}
			for _, c := range corpus {
				if cmp(b, c) != 0 {
					continue
				}
				if cmp(a, c) != 0 {
					t.Fatalf("equivalence transitivity violated: cmp(%#v,%#v)=0, cmp(%#v,%#v)=0, but cmp(%#v,%#v)=%d",
						a, b, b, c, a, c, cmp(a, c))
				}
			}
		}
	}

	// Transitivity of order: a ≡ b ∧ b < c ⟹ a < c.
	for _, a := range corpus {
		for _, b := range corpus {
			if cmp(a, b) != 0 {
				continue
			}
			for _, c := range corpus {
				if cmp(b, c) != -1 {
					continue
				}
				if cmp(a, c) != -1 {
					t.Fatalf("order transitivity (equiv) violated: cmp(%#v,%#v)=0, cmp(%#v,%#v)=-1, but cmp(%#v,%#v)=%d",
						a, b, b, c, a, c, cmp(a, c))
				}
			}
		}
	}

	// Transitivity of order: a < b ∧ b < c ⟹ a < c.
	for _, a := range corpus {
		for _, b := range corpus {
			if cmp(a, b) != -1 {
				continue
			}
			for _, c := range corpus {
				if cmp(b, c) != -1 {
					continue
				}
				if cmp(a, c) != -1 {
					t.Fatalf("order transitivity violated: cmp(%#v,%#v)=-1, cmp(%#v,%#v)=-1, but cmp(%#v,%#v)=%d",
						a, b, b, c, a, c, cmp(a, c))
				}
			}
		}
	}

	// NULL placement: nil sorts first and compares consistently.
	for _, v := range corpus {
		if v == nil {
			continue
		}
		if cmp(nil, v) != -1 || cmp(v, nil) != 1 {
			t.Fatalf("nil placement violated for %#v: cmp(nil,v)=%d cmp(v,nil)=%d", v, cmp(nil, v), cmp(v, nil))
		}
	}
}
