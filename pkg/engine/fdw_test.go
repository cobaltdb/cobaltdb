package engine

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestFDWManagerBasic(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	// Check built-in FDWs are registered
	if len(fm.registry) < 2 {
		t.Error("Expected at least 2 built-in FDWs")
	}
}

func TestCSVFDWBasic(t *testing.T) {
	// Create a temporary CSV file
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write test data
	content := "id,name,age\n1,Alice,30\n2,Bob,25\n3,Charlie,35\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	// Create CSV FDW
	fdw := NewCSVFDW()
	options := &FDWOptions{
		FilePath:   tmpFile.Name(),
		Delimiter:  ",",
		HeaderLine: true,
	}

	if err := fdw.Connect(context.Background(), options); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer fdw.Disconnect()

	// Scan the data
	iter, err := fdw.Scan(context.Background(), "test", []string{"id", "name", "age"}, FDWFilter{})
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	defer iter.Close()

	// Count rows
	count := 0
	for iter.Next() {
		row := iter.Row()
		if row == nil || len(row.Values) != 3 {
			t.Errorf("Expected 3 columns, got %d", len(row.Values))
		}
		count++
	}

	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}

	if err := iter.Err(); err != nil {
		t.Errorf("Iterator error: %v", err)
	}
}

func TestCSVFDWNoHeader(t *testing.T) {
	// Create a temporary CSV file without header
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := "1,Alice,30\n2,Bob,25\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	fdw := NewCSVFDW()
	options := &FDWOptions{
		FilePath:   tmpFile.Name(),
		Delimiter:  ",",
		HeaderLine: false,
	}

	if err := fdw.Connect(context.Background(), options); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer fdw.Disconnect()

	iter, err := fdw.Scan(context.Background(), "test", nil, FDWFilter{})
	if err != nil {
		t.Fatalf("Failed to scan: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}
}

func TestCSVFDWGetStats(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := "id,name\n1,Alice\n2,Bob\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	fdw := NewCSVFDW()
	options := &FDWOptions{
		FilePath: tmpFile.Name(),
	}

	fdw.Connect(context.Background(), options)
	defer fdw.Disconnect()

	stats, err := fdw.GetStats("test")
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.TableName != "test" {
		t.Errorf("Expected table name 'test', got '%s'", stats.TableName)
	}

	// Stats should have some estimated row count
	if stats.RowCount < 0 {
		t.Error("Expected non-negative row count")
	}
}

func TestCSVFDWRequiresFilePath(t *testing.T) {
	fdw := NewCSVFDW()
	options := &FDWOptions{
		FilePath: "",
	}

	err := fdw.Connect(context.Background(), options)
	if err == nil {
		t.Error("Expected error for missing file path")
	}
}

func TestCSVFDWOperationsNotSupported(t *testing.T) {
	fdw := NewCSVFDW()

	// Test Insert
	err := fdw.Insert(context.Background(), "test", &FDWRow{})
	if err == nil {
		t.Error("Expected error for INSERT")
	}

	// Test Update
	_, err = fdw.Update(context.Background(), "test", &FDWRow{}, FDWFilter{})
	if err == nil {
		t.Error("Expected error for UPDATE")
	}

	// Test Delete
	_, err = fdw.Delete(context.Background(), "test", FDWFilter{})
	if err == nil {
		t.Error("Expected error for DELETE")
	}
}

func TestFDWManagerCreateForeignTable(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	// Create a CSV file for testing
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	// Create FDW
	options := &FDWOptions{
		FilePath:   tmpFile.Name(),
		Delimiter:  ",",
		HeaderLine: true,
	}
	if err := fm.CreateFDW("my_csv", "csv", options); err != nil {
		t.Fatalf("Failed to create FDW: %v", err)
	}

	// Create foreign table
	columns := []FDWColumnDef{
		{Name: "id", Type: "INTEGER", Nullable: false},
		{Name: "name", Type: "TEXT", Nullable: true},
	}

	if err := fm.CreateForeignTable("external_users", "my_csv", options, columns); err != nil {
		t.Fatalf("Failed to create foreign table: %v", err)
	}

	// Verify table exists
	table, exists := fm.GetForeignTable("external_users")
	if !exists {
		t.Error("Expected foreign table to exist")
	}

	if table.Name != "external_users" {
		t.Errorf("Expected table name 'external_users', got '%s'", table.Name)
	}

	if len(table.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(table.Columns))
	}
}

func TestFDWManagerUnknownFDW(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	err := fm.CreateFDW("test", "unknown_type", &FDWOptions{})
	if err == nil {
		t.Error("Expected error for unknown FDW type")
	}
}

func TestFDWManagerDropForeignTable(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	// Create a temp file and FDW
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	options := &FDWOptions{FilePath: tmpFile.Name()}
	fm.CreateFDW("csv_fdw", "csv", options)
	fm.CreateForeignTable("test_table", "csv_fdw", options, nil)

	// Drop the table
	if err := fm.DropForeignTable("test_table"); err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	// Verify table is gone
	_, exists := fm.GetForeignTable("test_table")
	if exists {
		t.Error("Expected table to be dropped")
	}

	// Dropping non-existent table should error
	err = fm.DropForeignTable("test_table")
	if err == nil {
		t.Error("Expected error for dropping non-existent table")
	}
}

func TestFDWManagerListForeignTables(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	// Create temp file and FDW
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	options := &FDWOptions{FilePath: tmpFile.Name()}
	fm.CreateFDW("csv_fdw", "csv", options)

	// Initially empty
	tables := fm.ListForeignTables()
	if len(tables) != 0 {
		t.Errorf("Expected 0 tables initially, got %d", len(tables))
	}

	// Create some tables
	fm.CreateForeignTable("table1", "csv_fdw", options, nil)
	fm.CreateForeignTable("table2", "csv_fdw", options, nil)

	tables = fm.ListForeignTables()
	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}
}

func TestFDWCostEstimator(t *testing.T) {
	estimator := NewFDWCostEstimator()

	// Test with known stats
	stats := &FDWStats{
		RowCount: 1000,
	}

	cost := estimator.EstimateScanCost(stats, false)
	if cost <= 0 {
		t.Error("Expected positive cost")
	}

	// Test with filter pushdown (should be cheaper)
	costWithFilter := estimator.EstimateScanCost(stats, true)
	if costWithFilter >= cost {
		t.Error("Expected filter pushdown to reduce cost")
	}

	// Test with unknown stats
	unknownStats := &FDWStats{RowCount: -1}
	unknownCost := estimator.EstimateScanCost(unknownStats, false)
	if unknownCost <= 0 {
		t.Error("Expected positive cost for unknown stats")
	}
}

func TestFDWQueryPlanner(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	// Create temp file and FDW
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	content := "id,name\n1,Alice\n2,Bob\n"
	if _, err := tmpFile.WriteString(content); err != nil {
		t.Fatalf("Failed to write test data: %v", err)
	}
	tmpFile.Close()

	options := &FDWOptions{FilePath: tmpFile.Name()}
	fm.CreateFDW("csv_fdw", "csv", options)
	fm.CreateForeignTable("users", "csv_fdw", options, nil)

	planner := NewFDWQueryPlanner(fm)

	// Plan a simple query
	plan, err := planner.PlanSelect("users", []string{"id", "name"}, FDWFilter{})
	if err != nil {
		t.Fatalf("Failed to plan query: %v", err)
	}

	if plan.TableName != "users" {
		t.Errorf("Expected table name 'users', got '%s'", plan.TableName)
	}

	if plan.Cost <= 0 {
		t.Error("Expected positive cost")
	}

	// Plan with filter
	filter := FDWFilter{
		Column:   "id",
		Operator: "=",
		Value:    1,
	}
	planWithFilter, err := planner.PlanSelect("users", []string{"id"}, filter)
	if err != nil {
		t.Fatalf("Failed to plan query with filter: %v", err)
	}

	// Filter pushdown should be supported for CSV (but CSV doesn't actually support it)
	if planWithFilter.SupportsPushdown {
		t.Error("CSV FDW should not support pushdown")
	}
}

func TestFDWTransaction(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	// Create temp file and FDW
	tmpFile, err := os.CreateTemp("", "test_*.csv")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	options := &FDWOptions{FilePath: tmpFile.Name()}
	fm.CreateFDW("csv_fdw", "csv", options)

	// Create transaction
	txn := NewFDWTransaction(fm)

	// Begin transaction on FDW
	if err := txn.Begin("csv_fdw"); err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Commit should succeed
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit: %v", err)
	}

	// Begin again and rollback
	txn.Begin("csv_fdw")
	if err := txn.Rollback(); err != nil {
		t.Fatalf("Failed to rollback: %v", err)
	}
}

func TestFDWTransactionUnknownFDW(t *testing.T) {
	fm := NewFDWManager()
	defer fm.Close()

	txn := NewFDWTransaction(fm)
	err := txn.Begin("unknown_fdw")
	if err == nil {
		t.Error("Expected error for unknown FDW")
	}
}

func TestHTTPFDWBasic(t *testing.T) {
	fdw := NewHTTPFDW()
	options := &FDWOptions{
		URL:     "https://jsonplaceholder.typicode.com/posts",
		Method:  "GET",
		Timeout: 30 * time.Second,
		Headers: map[string]string{
			"Accept": "application/json",
		},
	}

	if err := fdw.Connect(context.Background(), options); err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer fdw.Disconnect()

	if fdw.Name() != "http" {
		t.Errorf("Expected FDW name 'http', got '%s'", fdw.Name())
	}

	if !fdw.SupportsPushdown() {
		t.Error("HTTP FDW should support pushdown")
	}
}

func TestHTTPFDWRequiresURL(t *testing.T) {
	fdw := NewHTTPFDW()
	options := &FDWOptions{
		URL: "",
	}

	err := fdw.Connect(context.Background(), options)
	if err == nil {
		t.Error("Expected error for missing URL")
	}
}

func TestHTTPFDWGetStats(t *testing.T) {
	fdw := NewHTTPFDW()
	options := &FDWOptions{
		URL:     "https://example.com/api",
		Timeout: 30 * time.Second,
	}

	fdw.Connect(context.Background(), options)
	defer fdw.Disconnect()

	stats, err := fdw.GetStats("test_table")
	if err != nil {
		t.Fatalf("Failed to get stats: %v", err)
	}

	if stats.RowCount != -1 {
		t.Error("Expected RowCount to be -1 (unknown) for HTTP FDW")
	}
}

func TestFDWFilter(t *testing.T) {
	// Test basic filter
	filter := FDWFilter{
		Column:   "id",
		Operator: "=",
		Value:    1,
	}

	if filter.Column != "id" {
		t.Errorf("Expected column 'id', got '%s'", filter.Column)
	}

	// Test compound filter
	compoundFilter := FDWFilter{
		Left:  &FDWFilter{Column: "id", Operator: "=", Value: 1},
		Right: &FDWFilter{Column: "name", Operator: "=", Value: "Alice"},
		Logic: "AND",
	}

	if compoundFilter.Logic != "AND" {
		t.Errorf("Expected logic 'AND', got '%s'", compoundFilter.Logic)
	}
}

func TestFDWColumnDef(t *testing.T) {
	col := FDWColumnDef{
		Name:     "id",
		Type:     "INTEGER",
		Nullable: false,
		Options: map[string]string{
			"primary_key": "true",
		},
	}

	if col.Name != "id" {
		t.Errorf("Expected name 'id', got '%s'", col.Name)
	}

	if col.Options["primary_key"] != "true" {
		t.Error("Expected primary_key option")
	}
}

func TestImportForeignSchema(t *testing.T) {
	ifs := &ImportForeignSchema{
		SourceName: "test_source",
		Options:    &FDWOptions{},
	}

	fdw := NewCSVFDW()
	metadata, err := ifs.Import(context.Background(), fdw)
	if err != nil {
		t.Fatalf("Failed to import schema: %v", err)
	}

	if metadata.Version != "1.0" {
		t.Errorf("Expected version '1.0', got '%s'", metadata.Version)
	}

	if metadata.Tables == nil {
		t.Error("Expected Tables map to be initialized")
	}
}

func TestFDWOptions(t *testing.T) {
	options := &FDWOptions{
		Host:       "localhost",
		Port:       5432,
		Database:   "mydb",
		Username:   "user",
		Password:   "pass",
		FilePath:   "/path/to/file.csv",
		Delimiter:  ",",
		HeaderLine: true,
		URL:        "https://api.example.com",
		Method:     "GET",
		Headers: map[string]string{
			"Authorization": "Bearer token",
		},
		Timeout: 30 * time.Second,
		Options: map[string]string{
			"encoding": "utf8",
		},
	}

	if options.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got '%s'", options.Host)
	}

	if options.Port != 5432 {
		t.Errorf("Expected port 5432, got %d", options.Port)
	}

	if options.Headers["Authorization"] != "Bearer token" {
		t.Error("Expected Authorization header")
	}
}
