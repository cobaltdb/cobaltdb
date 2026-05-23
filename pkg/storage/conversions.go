package storage

import "fmt"

func storageUint16(n int, name string) (uint16, error) {
	if n < 0 || n > 1<<16-1 {
		return 0, fmt.Errorf("%s exceeds uint16: %d", name, n)
	}
	return uint16(n), nil // #nosec G115 - range checked above.
}

func storageUint32(n int64, name string) (uint32, error) {
	if n < 0 || n > 1<<32-1 {
		return 0, fmt.Errorf("%s exceeds uint32: %d", name, n)
	}
	return uint32(n), nil // #nosec G115 - range checked above.
}

func storageUint64Offset(offset int64) (uint64, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative storage offset: %d", offset)
	}
	return uint64(offset), nil // #nosec G115 - range checked above.
}
