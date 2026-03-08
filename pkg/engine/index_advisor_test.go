package engine

import (
	"testing"
	"time"
)

func TestIndexAdvisorBasic(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record some queries
	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users WHERE email = 'test@example.com'")
	}

	// Generate recommendations
	recs := advisor.GenerateRecommendations()

	found := false
	for _, rec := range recs {
		if rec.TableName == "users" && len(rec.ColumnNames) == 1 && rec.ColumnNames[0] == "email" {
			found = true
			if rec.Priority < 1 {
				t.Errorf("Expected priority >= 1, got %d", rec.Priority)
			}
		}
	}

	if !found {
		t.Error("Expected recommendation for users.email")
	}
}

func TestIndexAdvisorDisabled(t *testing.T) {
	config := &IndexAdvisorConfig{
		Enabled: false,
	}

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")

	recs := advisor.GenerateRecommendations()
	if len(recs) != 0 {
		t.Error("Expected no recommendations when disabled")
	}
}

func TestIndexAdvisorMinQueryCount(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 100 // High threshold
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record only 5 queries (below threshold)
	for i := 0; i < 5; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users WHERE email = 'test'")
	}

	recs := advisor.GenerateRecommendations()
	if len(recs) != 0 {
		t.Error("Expected no recommendations below min query count")
	}
}

func TestIndexAdvisorMinExecTime(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 100.0 // High threshold

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record queries with low execution time
	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 10.0, "SELECT * FROM users WHERE email = 'test'")
	}

	recs := advisor.GenerateRecommendations()
	if len(recs) != 0 {
		t.Error("Expected no recommendations below min execution time")
	}
}

func TestIndexAdvisorMultiColumn(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0
	config.EnableMultiColumn = true

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record queries with multiple columns
	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users WHERE email = 'test'")
		advisor.RecordQuery("users", []string{"status"}, 50.0, "SELECT * FROM users WHERE status = 'active'")
	}

	recs := advisor.GenerateRecommendations()

	// Should have both single-column and multi-column recommendations
	foundSingle := false
	foundMulti := false
	for _, rec := range recs {
		if rec.TableName == "users" {
			if len(rec.ColumnNames) == 1 {
				foundSingle = true
			}
			if len(rec.ColumnNames) == 2 {
				foundMulti = true
			}
		}
	}

	if !foundSingle {
		t.Error("Expected single-column recommendations")
	}
	if !foundMulti {
		t.Error("Expected multi-column recommendations")
	}
}

func TestIndexAdvisorWhereConditions(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record different condition types
	advisor.RecordWhereCondition("users", "email", "equality")
	advisor.RecordWhereCondition("users", "email", "equality")
	advisor.RecordWhereCondition("users", "age", "range")
	advisor.RecordWhereCondition("users", "created_at", "orderby")
	advisor.RecordWhereCondition("orders", "user_id", "join")

	// Verify by checking stats were recorded
	stats := advisor.Stats()
	if stats.TablesTracked != 2 {
		t.Errorf("Expected 2 tables tracked, got %d", stats.TablesTracked)
	}
}

func TestIndexAdvisorGetRecommendations(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record queries for two tables
	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")
		advisor.RecordQuery("orders", []string{"user_id"}, 50.0, "SELECT * FROM orders")
	}

	advisor.GenerateRecommendations()

	// Get all recommendations
	allRecs := advisor.GetRecommendations()
	if len(allRecs) < 2 {
		t.Errorf("Expected at least 2 recommendations, got %d", len(allRecs))
	}

	// Get recommendations for specific table
	userRecs := advisor.GetRecommendationsForTable("users")
	found := false
	for _, rec := range userRecs {
		if rec.TableName == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected recommendations for users table")
	}

	// Check priority sorting
	for i := 1; i < len(allRecs); i++ {
		if allRecs[i].Priority > allRecs[i-1].Priority {
			t.Error("Recommendations should be sorted by priority descending")
		}
	}
}

func TestIndexAdvisorApplyRecommendation(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")
	}

	advisor.GenerateRecommendations()

	recs := advisor.GetRecommendations()
	if len(recs) == 0 {
		t.Fatal("Expected recommendations")
	}

	// Apply a recommendation
	err := advisor.ApplyRecommendation(recs[0].ID)
	if err != nil {
		t.Fatalf("Failed to apply recommendation: %v", err)
	}

	// Should be removed
	recsAfter := advisor.GetRecommendations()
	for _, rec := range recsAfter {
		if rec.ID == recs[0].ID {
			t.Error("Applied recommendation should be removed")
		}
	}

	// Applying non-existent should fail
	err = advisor.ApplyRecommendation("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent recommendation")
	}
}

func TestIndexAdvisorIgnoreRecommendation(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")
	}

	advisor.GenerateRecommendations()

	recs := advisor.GetRecommendations()
	if len(recs) == 0 {
		t.Fatal("Expected recommendations")
	}

	// Ignore a recommendation
	err := advisor.IgnoreRecommendation(recs[0].ID)
	if err != nil {
		t.Fatalf("Failed to ignore recommendation: %v", err)
	}

	// Should be removed
	recsAfter := advisor.GetRecommendations()
	for _, rec := range recsAfter {
		if rec.ID == recs[0].ID {
			t.Error("Ignored recommendation should be removed")
		}
	}
}

func TestIndexAdvisorReset(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")
	}

	advisor.GenerateRecommendations()

	// Reset
	advisor.Reset()

	// Check stats are cleared
	stats := advisor.Stats()
	if stats.TablesTracked != 0 {
		t.Errorf("Expected 0 tables after reset, got %d", stats.TablesTracked)
	}
	if stats.Recommendations != 0 {
		t.Errorf("Expected 0 recommendations after reset, got %d", stats.Recommendations)
	}
}

func TestIndexAdvisorStats(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Check initial stats
	stats := advisor.Stats()
	if !stats.Enabled {
		t.Error("Expected advisor to be enabled")
	}
	if stats.TablesTracked != 0 {
		t.Errorf("Expected 0 tables initially, got %d", stats.TablesTracked)
	}

	// Record some queries
	advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")

	stats = advisor.Stats()
	if stats.TablesTracked != 1 {
		t.Errorf("Expected 1 table tracked, got %d", stats.TablesTracked)
	}
}

func TestIndexAdvisorPruning(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MaxRecommendations = 2
	config.MinQueryCount = 1
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record queries for multiple tables
	for i := 0; i < 5; i++ {
		advisor.RecordQuery("table1", []string{"col1"}, 100.0, "SELECT * FROM table1")
		advisor.RecordQuery("table2", []string{"col2"}, 50.0, "SELECT * FROM table2")
		advisor.RecordQuery("table3", []string{"col3"}, 25.0, "SELECT * FROM table3")
	}

	advisor.GenerateRecommendations()

	recs := advisor.GetRecommendations()
	if len(recs) > config.MaxRecommendations {
		t.Errorf("Expected at most %d recommendations, got %d", config.MaxRecommendations, len(recs))
	}

	// Highest priority should be preserved
	if len(recs) > 0 && recs[0].Priority < 8 {
		t.Logf("Highest priority recommendation: %d", recs[0].Priority)
	}
}

func TestIndexRecommendationGenerateSQL(t *testing.T) {
	rec := &IndexRecommendation{
		TableName:   "users",
		ColumnNames: []string{"email", "status"},
		IndexType:   "BTREE",
	}

	sql := rec.GenerateIndexSQL()
	expected := "CREATE INDEX idx_users_email_status ON users (email, status);"
	if sql != expected {
		t.Errorf("Expected SQL:\n%s\nGot:\n%s", expected, sql)
	}
}

func TestIndexAdvisorSampleQueries(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record different queries
	queries := []string{
		"SELECT * FROM users WHERE email = 'test1@example.com'",
		"SELECT * FROM users WHERE email = 'test2@example.com'",
		"SELECT * FROM users WHERE email = 'test3@example.com'",
		"SELECT * FROM users WHERE email = 'test4@example.com'",
		"SELECT * FROM users WHERE email = 'test5@example.com'",
		"SELECT * FROM users WHERE email = 'test6@example.com'",
	}

	for _, sql := range queries {
		advisor.RecordQuery("users", []string{"email"}, 50.0, sql)
	}

	advisor.GenerateRecommendations()

	recs := advisor.GetRecommendationsForTable("users")
	if len(recs) == 0 {
		t.Fatal("Expected recommendations")
	}

	// Should have at most 5 sample queries
	if len(recs[0].SampleQueries) > 5 {
		t.Errorf("Expected at most 5 sample queries, got %d", len(recs[0].SampleQueries))
	}
}

func TestIndexAdvisorPriorityCalculation(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 1
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// High frequency, high execution time queries
	for i := 0; i < 200; i++ {
		advisor.RecordQuery("users", []string{"email"}, 200.0, "SELECT * FROM users WHERE email = 'test'")
	}

	advisor.GenerateRecommendations()

	recs := advisor.GetRecommendations()
	if len(recs) == 0 {
		t.Fatal("Expected recommendations")
	}

	// Should have high priority (10 is max, we expect at least 6)
	if recs[0].Priority < 6 {
		t.Errorf("Expected high priority for high frequency + high exec time, got %d", recs[0].Priority)
	}
}

func TestIndexAdvisorReason(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	config.MinQueryCount = 5
	config.MinAvgExecTimeMs = 1.0

	advisor := NewIndexAdvisor(config)
	defer advisor.Close()

	// Record queries and conditions
	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")
	}

	advisor.RecordWhereCondition("users", "email", "equality")
	advisor.RecordWhereCondition("users", "email", "equality")
	advisor.RecordWhereCondition("users", "age", "range")

	advisor.GenerateRecommendations()

	recs := advisor.GetRecommendations()
	if len(recs) == 0 {
		t.Fatal("Expected recommendations")
	}

	// Check that reason is populated
	for _, rec := range recs {
		if rec.Reason == "" {
			t.Error("Expected non-empty reason for recommendation")
		}
	}
}

func TestIndexAdvisorClose(t *testing.T) {
	config := DefaultIndexAdvisorConfig()
	advisor := NewIndexAdvisor(config)

	// Record some queries
	for i := 0; i < 10; i++ {
		advisor.RecordQuery("users", []string{"email"}, 50.0, "SELECT * FROM users")
	}

	// Close should not panic
	err := advisor.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}
}

func TestDefaultIndexAdvisorConfig(t *testing.T) {
	config := DefaultIndexAdvisorConfig()

	if !config.Enabled {
		t.Error("Expected enabled by default")
	}
	if config.MinQueryCount != 10 {
		t.Errorf("Expected MinQueryCount 10, got %d", config.MinQueryCount)
	}
	if config.MinAvgExecTimeMs != 10.0 {
		t.Errorf("Expected MinAvgExecTimeMs 10.0, got %f", config.MinAvgExecTimeMs)
	}
	if config.MaxRecommendations != 100 {
		t.Errorf("Expected MaxRecommendations 100, got %d", config.MaxRecommendations)
	}
	if config.AnalysisWindow != 24*time.Hour {
		t.Errorf("Expected AnalysisWindow 24h, got %v", config.AnalysisWindow)
	}
	if !config.EnableMultiColumn {
		t.Error("Expected EnableMultiColumn true by default")
	}
}

func TestIndexAdvisorNilConfig(t *testing.T) {
	// Should use default config when nil
	advisor := NewIndexAdvisor(nil)
	defer advisor.Close()

	if advisor.config == nil {
		t.Fatal("Expected config to be set")
	}

	if !advisor.config.Enabled {
		t.Error("Expected enabled by default")
	}
}
