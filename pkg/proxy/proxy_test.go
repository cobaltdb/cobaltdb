package proxy

import (
	"fmt"
	"strings"
	"testing"
)

func TestDefaultProxyConfig(t *testing.T) {
	config := DefaultProxyConfig()

	if config.ListenPort != 5433 {
		t.Errorf("Expected listen port 5433, got %d", config.ListenPort)
	}

	if config.MaxConnections != 1000 {
		t.Errorf("Expected max connections 1000, got %d", config.MaxConnections)
	}

	if config.LoadBalanceStrategy != "round_robin" {
		t.Errorf("Expected round_robin strategy, got %s", config.LoadBalanceStrategy)
	}

	if !config.EnableReadWriteSplit {
		t.Error("Expected read-write split to be enabled by default")
	}
}

func TestBackendAddress(t *testing.T) {
	backend := &Backend{
		Host: "localhost",
		Port: 5432,
	}

	addr := backend.Address()
	if addr != "localhost:5432" {
		t.Errorf("Expected address 'localhost:5432', got '%s'", addr)
	}
}

func TestBackendHealth(t *testing.T) {
	backend := &Backend{
		ID:   "test1",
		Host: "localhost",
		Port: 5432,
	}

	if backend.IsHealthy() {
		t.Error("New backend should not be healthy by default")
	}

	backend.SetHealth(true)
	if !backend.IsHealthy() {
		t.Error("Backend should be healthy after setting")
	}

	backend.SetHealth(false)
	if backend.IsHealthy() {
		t.Error("Backend should not be healthy after setting false")
	}
}

func TestBackendConnections(t *testing.T) {
	backend := &Backend{
		ID:   "test1",
		Host: "localhost",
		Port: 5432,
	}

	backend.IncrementConnections()
	if backend.ActiveConns != 1 {
		t.Errorf("Expected 1 active connection, got %d", backend.ActiveConns)
	}
	if backend.TotalConns != 1 {
		t.Errorf("Expected 1 total connection, got %d", backend.TotalConns)
	}

	backend.IncrementConnections()
	if backend.ActiveConns != 2 {
		t.Errorf("Expected 2 active connections, got %d", backend.ActiveConns)
	}

	backend.DecrementConnections()
	if backend.ActiveConns != 1 {
		t.Errorf("Expected 1 active connection after decrement, got %d", backend.ActiveConns)
	}

	// Total should not decrease
	if backend.TotalConns != 2 {
		t.Errorf("Expected 2 total connections, got %d", backend.TotalConns)
	}
}

func TestSQLProxyAddBackend(t *testing.T) {
	proxy := NewSQLProxy(nil)

	// Valid backend
	backend := &Backend{
		ID:   "primary1",
		Host: "localhost",
		Port: 5432,
		Type: BackendTypePrimary,
	}

	err := proxy.AddBackend(backend)
	if err != nil {
		t.Fatalf("Failed to add backend: %v", err)
	}

	// Backend without ID
	invalidBackend := &Backend{
		Host: "localhost",
		Port: 5433,
	}
	err = proxy.AddBackend(invalidBackend)
	if err == nil {
		t.Error("Expected error for backend without ID")
	}

	// Backend without host
	invalidBackend2 := &Backend{
		ID:   "test2",
		Port: 5433,
	}
	err = proxy.AddBackend(invalidBackend2)
	if err == nil {
		t.Error("Expected error for backend without host")
	}

	// Backend without port
	invalidBackend3 := &Backend{
		ID:   "test3",
		Host: "localhost",
	}
	err = proxy.AddBackend(invalidBackend3)
	if err == nil {
		t.Error("Expected error for backend without port")
	}
}

func TestSQLProxyRemoveBackend(t *testing.T) {
	proxy := NewSQLProxy(nil)

	backend := &Backend{
		ID:   "primary1",
		Host: "localhost",
		Port: 5432,
		Type: BackendTypePrimary,
	}
	proxy.AddBackend(backend)

	err := proxy.RemoveBackend("primary1")
	if err != nil {
		t.Fatalf("Failed to remove backend: %v", err)
	}

	// Should be removed
	proxy.mu.RLock()
	_, exists := proxy.backends["primary1"]
	proxy.mu.RUnlock()
	if exists {
		t.Error("Backend should be removed")
	}

	// Remove non-existent
	err = proxy.RemoveBackend("non_existent")
	if err == nil {
		t.Error("Expected error for non-existent backend")
	}
}

func TestSQLProxySelectBackendPrimary(t *testing.T) {
	config := DefaultProxyConfig()
	config.EnableReadWriteSplit = true
	proxy := NewSQLProxy(config)

	primary := &Backend{
		ID:      "primary",
		Host:    "localhost",
		Port:    5432,
		Type:    BackendTypePrimary,
		Healthy: true,
	}
	replica := &Backend{
		ID:      "replica1",
		Host:    "localhost",
		Port:    5433,
		Type:    BackendTypeReplica,
		Healthy: true,
	}

	proxy.AddBackend(primary)
	proxy.AddBackend(replica)

	// Write should go to primary
	backend := proxy.selectBackend(true)
	if backend == nil {
		t.Fatal("Expected a backend for write")
	}
	if backend.ID != "primary" {
		t.Errorf("Expected primary for write, got %s", backend.ID)
	}

	// Read should go to replica
	backend = proxy.selectBackend(false)
	if backend == nil {
		t.Fatal("Expected a backend for read")
	}
	if backend.ID != "replica1" {
		t.Errorf("Expected replica for read, got %s", backend.ID)
	}
}

func TestSQLProxySelectBackendRoundRobin(t *testing.T) {
	config := DefaultProxyConfig()
	config.LoadBalanceStrategy = "round_robin"
	proxy := NewSQLProxy(config)

	for i := 0; i < 3; i++ {
		backend := &Backend{
			ID:      fmt.Sprintf("replica%d", i),
			Host:    "localhost",
			Port:    5433 + i,
			Type:    BackendTypeReplica,
			Healthy: true,
		}
		proxy.AddBackend(backend)
	}

	// Test round-robin
	selected := make(map[string]int)
	for i := 0; i < 6; i++ {
		b := proxy.selectBackend(false)
		if b != nil {
			selected[b.ID]++
		}
	}

	// Each backend should be selected twice
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("replica%d", i)
		if selected[id] != 2 {
			t.Errorf("Expected %s to be selected 2 times, got %d", id, selected[id])
		}
	}
}

func TestSQLProxySelectBackendLeastConnections(t *testing.T) {
	config := DefaultProxyConfig()
	config.LoadBalanceStrategy = "least_connections"
	proxy := NewSQLProxy(config)

	for i := 0; i < 3; i++ {
		backend := &Backend{
			ID:      fmt.Sprintf("replica%d", i),
			Host:    "localhost",
			Port:    5433 + i,
			Type:    BackendTypeReplica,
			Healthy: true,
		}
		proxy.AddBackend(backend)
	}

	// Set different connection counts
	for i := 0; i < 3; i++ {
		proxy.replicas[i].ActiveConns = int32(i * 10) // 0, 10, 20
	}

	// Should select the one with least connections (replica0)
	backend := proxy.selectBackend(false)
	if backend == nil || backend.ID != "replica0" {
		t.Errorf("Expected replica0 (least connections), got %v", backend)
	}
}

func TestSQLProxySelectBackendWeighted(t *testing.T) {
	config := DefaultProxyConfig()
	config.LoadBalanceStrategy = "weighted"
	proxy := NewSQLProxy(config)

	for i := 0; i < 3; i++ {
		backend := &Backend{
			ID:      fmt.Sprintf("replica%d", i),
			Host:    "localhost",
			Port:    5433 + i,
			Type:    BackendTypeReplica,
			Healthy: true,
			Weight:  i + 1, // 1, 2, 3
		}
		proxy.AddBackend(backend)
	}

	// Test weighted selection
	selected := make(map[string]int)
	for i := 0; i < 60; i++ {
		b := proxy.selectBackend(false)
		if b != nil {
			selected[b.ID]++
		}
	}

	// replica2 (weight 3) should be selected most, replica0 (weight 1) least
	if selected["replica2"] <= selected["replica0"] {
		t.Errorf("Weighted selection not working: replica2=%d, replica0=%d",
			selected["replica2"], selected["replica0"])
	}
}

func TestSQLProxyIsWriteQuery(t *testing.T) {
	proxy := NewSQLProxy(nil)

	writeQueries := []string{
		"INSERT INTO users VALUES (1)",
		"UPDATE users SET name = 'test'",
		"DELETE FROM users",
		"CREATE TABLE test (id INT)",
		"DROP TABLE test",
		"ALTER TABLE users ADD COLUMN age INT",
		"TRUNCATE TABLE logs",
		"GRANT SELECT ON users TO app",
		"REVOKE SELECT ON users FROM app",
	}

	for _, query := range writeQueries {
		if !proxy.isWriteQuery(query) {
			t.Errorf("Expected '%s' to be a write query", query)
		}
	}

	readQueries := []string{
		"SELECT * FROM users",
		"select id from orders",
		"SELECT COUNT(*) FROM products",
	}

	for _, query := range readQueries {
		if proxy.isWriteQuery(query) {
			t.Errorf("Expected '%s' to be a read query", query)
		}
	}
}

func TestSQLProxyGetStats(t *testing.T) {
	proxy := NewSQLProxy(nil)

	backend1 := &Backend{
		ID:      "primary",
		Host:    "localhost",
		Port:    5432,
		Type:    BackendTypePrimary,
		Healthy: true,
	}
	backend2 := &Backend{
		ID:      "replica",
		Host:    "localhost",
		Port:    5433,
		Type:    BackendTypeReplica,
		Healthy: false,
	}

	proxy.AddBackend(backend1)
	proxy.AddBackend(backend2)

	stats := proxy.GetStats()

	if stats.TotalBackends != 2 {
		t.Errorf("Expected 2 backends, got %d", stats.TotalBackends)
	}

	if stats.HealthyBackends != 1 {
		t.Errorf("Expected 1 healthy backend, got %d", stats.HealthyBackends)
	}

	if len(stats.BackendStats) != 2 {
		t.Errorf("Expected 2 backend stats, got %d", len(stats.BackendStats))
	}
}

func TestGenerateConnID(t *testing.T) {
	id1 := generateConnID()
	id2 := generateConnID()

	if id1 == id2 {
		t.Error("Connection IDs should be unique")
	}

	if !strings.HasPrefix(id1, "conn_") {
		t.Errorf("Connection ID should start with 'conn_', got %s", id1)
	}

	// Both should have the format conn_<timestamp>_<counter>
	parts1 := strings.Split(id1, "_")
	if len(parts1) != 3 {
		t.Errorf("Connection ID should have 3 parts, got %d", len(parts1))
	}
}
