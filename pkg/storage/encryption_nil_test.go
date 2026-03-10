package storage

import "testing"

func TestNewEncryptedBackendNilConfig(t *testing.T) {
	backend := NewMemory()
	defer backend.Close()

	_, err := NewEncryptedBackend(backend, nil)
	if err == nil {
		t.Fatal("expected error for nil encryption config")
	}
}
