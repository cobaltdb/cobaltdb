package storage

import "fmt"

func checkedUint16(n int, name string) (uint16, error) {
	if n < 0 || n > 1<<16-1 {
		return 0, fmt.Errorf("%s exceeds uint16: %d", name, n)
	}
	return uint16(n), nil // #nosec G115 - range checked above.
}

func checkedUint32(n int64, name string) (uint32, error) {
	if n < 0 || n > 1<<32-1 {
		return 0, fmt.Errorf("%s exceeds uint32: %d", name, n)
	}
	return uint32(n), nil // #nosec G115 - range checked above.
}
