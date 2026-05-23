package catalog

import "fmt"

func catalogUint64Count(n int64, name string) (uint64, error) {
	if n < 0 {
		return 0, fmt.Errorf("%s cannot be negative: %d", name, n)
	}
	return uint64(n), nil // #nosec G115 - range checked above.
}

func catalogUint64Len(n int, name string) (uint64, error) {
	if n < 0 {
		return 0, fmt.Errorf("%s cannot be negative: %d", name, n)
	}
	return uint64(n), nil // #nosec G115 - range checked above.
}
