package engine

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"
)

// FDWOptions contains options for foreign data wrappers
type FDWOptions struct {
	// Connection options
	Host     string
	Port     int
	Database string
	Username string
	Password string
	SSLMode  string

	// File-based options
	FilePath   string
	Delimiter  string
	HeaderLine bool
	Encoding   string

	// HTTP options
	URL     string
	Method  string
	Headers map[string]string
	Timeout time.Duration

	// Generic options
	Options map[string]string
}

// FDWRow represents a row from a foreign data source
type FDWRow struct {
	Values []interface{}
}

// FDWIterator provides iteration over foreign data
type FDWIterator interface {
	// Next advances to the next row
	Next() bool

	// Row returns the current row
	Row() *FDWRow

	// Err returns any error encountered during iteration
	Err() error

	// Close closes the iterator
	Close() error
}

// FDW interface that all foreign data wrappers must implement
type FDW interface {
	// Name returns the FDW name
	Name() string

	// Connect establishes connection to the foreign data source
	Connect(ctx context.Context, options *FDWOptions) error

	// Disconnect closes the connection
	Disconnect() error

	// Scan returns an iterator over the foreign data
	Scan(ctx context.Context, table string, columns []string, filter FDWFilter) (FDWIterator, error)

	// Insert inserts a row into the foreign table (optional)
	Insert(ctx context.Context, table string, row *FDWRow) error

	// Update updates rows in the foreign table (optional)
	Update(ctx context.Context, table string, row *FDWRow, filter FDWFilter) (int64, error)

	// Delete deletes rows from the foreign table (optional)
	Delete(ctx context.Context, table string, filter FDWFilter) (int64, error)

	// GetStats returns statistics about the foreign table
	GetStats(table string) (*FDWStats, error)

	// SupportsPushdown returns true if the FDW supports filter pushdown
	SupportsPushdown() bool
}

// FDWFilter represents a filter condition for pushdown
type FDWFilter struct {
	// Column to filter on
	Column string

	// Operator: =, <>, <, >, <=, >=, LIKE, IN
	Operator string

	// Value to compare against
	Value interface{}

	// Values for IN operator
	Values []interface{}

	// Compound filters (AND/OR)
	Left  *FDWFilter
	Right *FDWFilter
	Logic string // AND, OR
}

// FDWStats contains statistics about a foreign table
type FDWStats struct {
	TableName    string
	RowCount     int64
	ColumnStats  map[string]*FDWColumnStats
	LastAnalyzed time.Time
}

// FDWColumnStats contains statistics about a column
type FDWColumnStats struct {
	ColumnName   string
	DistinctVals int64
	NullFrac     float64
	AvgWidth     int
	MinVal       interface{}
	MaxVal       interface{}
}

// FDWTable represents a foreign table definition
type FDWTable struct {
	Name    string
	FDWName string
	Options *FDWOptions
	Columns []FDWColumnDef
}

// FDWColumnDef defines a column in a foreign table
type FDWColumnDef struct {
	Name     string
	Type     string
	Nullable bool
	Options  map[string]string
}

// FDWManager manages foreign data wrappers
type FDWManager struct {
	mu       sync.RWMutex
	fdws     map[string]FDW        // FDW name -> FDW instance
	tables   map[string]*FDWTable  // table name -> table definition
	registry map[string]FDWFactory // FDW type -> factory
}

// FDWFactory creates FDW instances
type FDWFactory func() FDW

// NewFDWManager creates a new FDW manager
func NewFDWManager() *FDWManager {
	fm := &FDWManager{
		fdws:     make(map[string]FDW),
		tables:   make(map[string]*FDWTable),
		registry: make(map[string]FDWFactory),
	}

	// Register built-in FDWs
	fm.RegisterFDW("csv", func() FDW { return NewCSVFDW() })
	fm.RegisterFDW("http", func() FDW { return NewHTTPFDW() })

	return fm
}

// RegisterFDW registers a new FDW type
func (fm *FDWManager) RegisterFDW(name string, factory FDWFactory) {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	fm.registry[name] = factory
}

// CreateFDW creates a new FDW instance
func (fm *FDWManager) CreateFDW(name string, fdwType string, options *FDWOptions) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	factory, exists := fm.registry[fdwType]
	if !exists {
		return fmt.Errorf("unknown FDW type: %s", fdwType)
	}

	fdw := factory()
	if err := fdw.Connect(context.Background(), options); err != nil {
		return fmt.Errorf("failed to connect FDW: %w", err)
	}

	fm.fdws[name] = fdw
	return nil
}

// CreateForeignTable creates a foreign table
func (fm *FDWManager) CreateForeignTable(name string, fdwName string, options *FDWOptions, columns []FDWColumnDef) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if _, exists := fm.fdws[fdwName]; !exists {
		return fmt.Errorf("FDW not found: %s", fdwName)
	}

	fm.tables[name] = &FDWTable{
		Name:    name,
		FDWName: fdwName,
		Options: options,
		Columns: columns,
	}

	return nil
}

// DropForeignTable drops a foreign table
func (fm *FDWManager) DropForeignTable(name string) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if _, exists := fm.tables[name]; !exists {
		return fmt.Errorf("foreign table not found: %s", name)
	}

	delete(fm.tables, name)
	return nil
}

// QueryForeignTable queries a foreign table
func (fm *FDWManager) QueryForeignTable(ctx context.Context, tableName string, columns []string, filter FDWFilter) (FDWIterator, error) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	table, exists := fm.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("foreign table not found: %s", tableName)
	}

	fdw, exists := fm.fdws[table.FDWName]
	if !exists {
		return nil, fmt.Errorf("FDW not found: %s", table.FDWName)
	}

	return fdw.Scan(ctx, tableName, columns, filter)
}

// GetForeignTable returns a foreign table definition
func (fm *FDWManager) GetForeignTable(name string) (*FDWTable, bool) {
	fm.mu.RLock()
	defer fm.mu.RUnlock()
	table, exists := fm.tables[name]
	return table, exists
}

// ListForeignTables returns all foreign table names
func (fm *FDWManager) ListForeignTables() []string {
	fm.mu.RLock()
	defer fm.mu.RUnlock()

	names := make([]string, 0, len(fm.tables))
	for name := range fm.tables {
		names = append(names, name)
	}
	return names
}

// Close closes all FDW connections
func (fm *FDWManager) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	var errs []error
	for name, fdw := range fm.fdws {
		if err := fdw.Disconnect(); err != nil {
			errs = append(errs, fmt.Errorf("failed to disconnect FDW %s: %w", name, err))
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// CSVFDW implements FDW for CSV files
type CSVFDW struct {
	options *FDWOptions
	file    *os.File
	mu      sync.Mutex
}

// NewCSVFDW creates a new CSV FDW
func NewCSVFDW() FDW {
	return &CSVFDW{}
}

// Name returns the FDW name
func (c *CSVFDW) Name() string {
	return "csv"
}

// Connect establishes connection to the CSV file
func (c *CSVFDW) Connect(ctx context.Context, options *FDWOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if options.FilePath == "" {
		return errors.New("file path is required for CSV FDW")
	}

	c.options = options
	return nil
}

// Disconnect closes the CSV file
func (c *CSVFDW) Disconnect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.file != nil {
		err := c.file.Close()
		c.file = nil
		return err
	}
	return nil
}

// Scan returns an iterator over the CSV data
func (c *CSVFDW) Scan(ctx context.Context, table string, columns []string, filter FDWFilter) (FDWIterator, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	file, err := os.Open(c.options.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}

	delimiter := c.options.Delimiter
	if delimiter == "" {
		delimiter = ","
	}

	return &CSVIterator{
		reader:     csv.NewReader(file),
		file:       file,
		filter:     filter,
		delimiter:  rune(delimiter[0]),
		headerLine: c.options.HeaderLine,
	}, nil
}

// Insert inserts a row (not supported for CSV)
func (c *CSVFDW) Insert(ctx context.Context, table string, row *FDWRow) error {
	return errors.New("CSV FDW does not support INSERT")
}

// Update updates rows (not supported for CSV)
func (c *CSVFDW) Update(ctx context.Context, table string, row *FDWRow, filter FDWFilter) (int64, error) {
	return 0, errors.New("CSV FDW does not support UPDATE")
}

// Delete deletes rows (not supported for CSV)
func (c *CSVFDW) Delete(ctx context.Context, table string, filter FDWFilter) (int64, error) {
	return 0, errors.New("CSV FDW does not support DELETE")
}

// GetStats returns statistics about the CSV file
func (c *CSVFDW) GetStats(table string) (*FDWStats, error) {
	info, err := os.Stat(c.options.FilePath)
	if err != nil {
		return nil, err
	}

	// Estimate row count based on file size (rough approximation)
	estimatedRows := info.Size() / 100 // Assume average 100 bytes per row

	return &FDWStats{
		TableName:    table,
		RowCount:     estimatedRows,
		ColumnStats:  make(map[string]*FDWColumnStats),
		LastAnalyzed: time.Now(),
	}, nil
}

// SupportsPushdown returns false for CSV FDW
func (c *CSVFDW) SupportsPushdown() bool {
	return false
}

// CSVIterator iterates over CSV data
type CSVIterator struct {
	reader     *csv.Reader
	file       *os.File
	filter     FDWFilter
	delimiter  rune
	headerLine bool
	headers    []string
	current    *FDWRow
	err        error
}

// Next advances to the next row
func (ci *CSVIterator) Next() bool {
	if ci.err != nil {
		return false
	}

	record, err := ci.reader.Read()
	if err == io.EOF {
		return false
	}
	if err != nil {
		ci.err = err
		return false
	}

	// Handle header line
	if ci.headers == nil && ci.headerLine {
		ci.headers = record
		return ci.Next() // Skip header and read next
	}

	// Convert record to FDWRow
	values := make([]interface{}, len(record))
	for i, v := range record {
		values[i] = v
	}

	ci.current = &FDWRow{Values: values}
	return true
}

// Row returns the current row
func (ci *CSVIterator) Row() *FDWRow {
	return ci.current
}

// Err returns any error
func (ci *CSVIterator) Err() error {
	return ci.err
}

// Close closes the iterator
func (ci *CSVIterator) Close() error {
	return ci.file.Close()
}

// HTTPFDW implements FDW for HTTP REST APIs
type HTTPFDW struct {
	options    *FDWOptions
	httpClient *http.Client
	mu         sync.Mutex
}

// NewHTTPFDW creates a new HTTP FDW
func NewHTTPFDW() FDW {
	return &HTTPFDW{}
}

// Name returns the FDW name
func (h *HTTPFDW) Name() string {
	return "http"
}

// Connect establishes connection to the HTTP endpoint
func (h *HTTPFDW) Connect(ctx context.Context, options *FDWOptions) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if options.URL == "" {
		return errors.New("URL is required for HTTP FDW")
	}

	h.options = options

	timeout := options.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	h.httpClient = &http.Client{
		Timeout: timeout,
	}

	return nil
}

// Disconnect closes the HTTP client
func (h *HTTPFDW) Disconnect() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.httpClient = nil
	return nil
}

// Scan returns an iterator over the HTTP data
func (h *HTTPFDW) Scan(ctx context.Context, table string, columns []string, filter FDWFilter) (FDWIterator, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	method := h.options.Method
	if method == "" {
		method = "GET"
	}

	// Build URL with filters if supported
	url := h.options.URL
	if filter.Column != "" && filter.Operator == "=" {
		url = fmt.Sprintf("%s?%s=%v", url, filter.Column, filter.Value)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers
	for k, v := range h.options.Headers {
		req.Header.Set(k, v)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	return &HTTPIterator{
		response: resp,
		decoder:  json.NewDecoder(resp.Body),
	}, nil
}

// Insert performs a POST request
func (h *HTTPFDW) Insert(ctx context.Context, table string, row *FDWRow) error {
	return errors.New("HTTP FDW does not support INSERT")
}

// Update performs a PUT/PATCH request
func (h *HTTPFDW) Update(ctx context.Context, table string, row *FDWRow, filter FDWFilter) (int64, error) {
	return 0, errors.New("HTTP FDW does not support UPDATE")
}

// Delete performs a DELETE request
func (h *HTTPFDW) Delete(ctx context.Context, table string, filter FDWFilter) (int64, error) {
	return 0, errors.New("HTTP FDW does not support DELETE")
}

// GetStats returns statistics (not available for HTTP)
func (h *HTTPFDW) GetStats(table string) (*FDWStats, error) {
	return &FDWStats{
		TableName:    table,
		RowCount:     -1, // Unknown
		ColumnStats:  make(map[string]*FDWColumnStats),
		LastAnalyzed: time.Now(),
	}, nil
}

// SupportsPushdown returns true for simple equality filters
func (h *HTTPFDW) SupportsPushdown() bool {
	return true
}

// HTTPIterator iterates over JSON array response
type HTTPIterator struct {
	response *http.Response
	decoder  *json.Decoder
	current  *FDWRow
	err      error
	results  []interface{}
	index    int
}

// Next advances to the next row
func (hi *HTTPIterator) Next() bool {
	if hi.err != nil {
		return false
	}

	// First call: decode the entire response
	if hi.results == nil {
		var data interface{}
		if err := hi.decoder.Decode(&data); err != nil {
			hi.err = err
			return false
		}

		// Handle both array and object responses
		switch v := data.(type) {
		case []interface{}:
			hi.results = v
		case map[string]interface{}:
			// Try to find an array field
			for _, val := range v {
				if arr, ok := val.([]interface{}); ok {
					hi.results = arr
					break
				}
			}
			if hi.results == nil {
				hi.results = []interface{}{v}
			}
		default:
			hi.err = errors.New("unsupported JSON structure")
			return false
		}
	}

	if hi.index >= len(hi.results) {
		return false
	}

	// Convert current result to FDWRow
	row := hi.results[hi.index]
	hi.index++

	if m, ok := row.(map[string]interface{}); ok {
		// Convert map to ordered values
		values := make([]interface{}, 0, len(m))
		for _, v := range m {
			values = append(values, v)
		}
		hi.current = &FDWRow{Values: values}
	} else {
		hi.current = &FDWRow{Values: []interface{}{row}}
	}

	return true
}

// Row returns the current row
func (hi *HTTPIterator) Row() *FDWRow {
	return hi.current
}

// Err returns any error
func (hi *HTTPIterator) Err() error {
	return hi.err
}

// Close closes the iterator
func (hi *HTTPIterator) Close() error {
	return hi.response.Body.Close()
}

// FDWCostEstimator estimates the cost of FDW operations
type FDWCostEstimator struct {
	defaultNetworkCost float64
	defaultRowCost     float64
}

// NewFDWCostEstimator creates a new cost estimator
func NewFDWCostEstimator() *FDWCostEstimator {
	return &FDWCostEstimator{
		defaultNetworkCost: 100.0, // Base cost for network operation
		defaultRowCost:     0.01,  // Cost per row
	}
}

// EstimateScanCost estimates the cost of scanning a foreign table
func (ce *FDWCostEstimator) EstimateScanCost(stats *FDWStats, hasFilter bool) float64 {
	cost := ce.defaultNetworkCost

	if stats != nil && stats.RowCount > 0 {
		cost += float64(stats.RowCount) * ce.defaultRowCost
	} else {
		// Unknown size, assume large
		cost += 10000 * ce.defaultRowCost
	}

	if hasFilter {
		// Filter pushdown reduces cost
		cost *= 0.5
	}

	return cost
}

// FDWQueryPlanner plans queries for foreign tables
type FDWQueryPlanner struct {
	manager   *FDWManager
	estimator *FDWCostEstimator
}

// NewFDWQueryPlanner creates a new FDW query planner
func NewFDWQueryPlanner(manager *FDWManager) *FDWQueryPlanner {
	return &FDWQueryPlanner{
		manager:   manager,
		estimator: NewFDWCostEstimator(),
	}
}

// PlanSelect plans a SELECT query on a foreign table
func (qp *FDWQueryPlanner) PlanSelect(tableName string, columns []string, filter FDWFilter) (*FDWPlan, error) {
	table, exists := qp.manager.GetForeignTable(tableName)
	if !exists {
		return nil, fmt.Errorf("foreign table not found: %s", tableName)
	}

	fdw, exists := qp.manager.fdws[table.FDWName]
	if !exists {
		return nil, fmt.Errorf("FDW not found: %s", table.FDWName)
	}

	stats, _ := fdw.GetStats(tableName)
	cost := qp.estimator.EstimateScanCost(stats, filter.Column != "")

	plan := &FDWPlan{
		TableName:        tableName,
		Columns:          columns,
		Filter:           filter,
		Cost:             cost,
		RowEstimate:      stats.RowCount,
		SupportsPushdown: fdw.SupportsPushdown() && filter.Column != "",
	}

	return plan, nil
}

// FDWPlan represents a query plan for a foreign table
type FDWPlan struct {
	TableName        string
	Columns          []string
	Filter           FDWFilter
	Cost             float64
	RowEstimate      int64
	SupportsPushdown bool
}

// FDWTransaction manages transactions for FDW operations
type FDWTransaction struct {
	manager *FDWManager
	fdws    map[string]FDW // FDWs participating in transaction
	mu      sync.Mutex
}

// NewFDWTransaction creates a new FDW transaction
func NewFDWTransaction(manager *FDWManager) *FDWTransaction {
	return &FDWTransaction{
		manager: manager,
		fdws:    make(map[string]FDW),
	}
}

// Begin begins a transaction on the specified FDW
func (ft *FDWTransaction) Begin(fdwName string) error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	fdw, exists := ft.manager.fdws[fdwName]
	if !exists {
		return fmt.Errorf("FDW not found: %s", fdwName)
	}

	ft.fdws[fdwName] = fdw
	return nil
}

// Commit commits the transaction
func (ft *FDWTransaction) Commit() error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	// HTTP and CSV FDWs don't support transactions
	// This would be implemented for database-backed FDWs
	ft.fdws = make(map[string]FDW)
	return nil
}

// Rollback rolls back the transaction
func (ft *FDWTransaction) Rollback() error {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	ft.fdws = make(map[string]FDW)
	return nil
}

// FDWMetadata stores metadata about foreign tables
type FDWMetadata struct {
	Version      string
	Tables       map[string]*FDWTableInfo
	LastModified time.Time
}

// FDWTableInfo stores metadata about a foreign table
type FDWTableInfo struct {
	Name        string
	FDWName     string
	ColumnNames []string
	ColumnTypes []string
	CreatedAt   time.Time
}

// ImportForeignSchema imports schema from a foreign source
type ImportForeignSchema struct {
	SourceName string
	Options    *FDWOptions
}

// Import imports schema from a foreign source
func (ifs *ImportForeignSchema) Import(ctx context.Context, fdw FDW) (*FDWMetadata, error) {
	// This would connect to the foreign source and introspect its schema
	// For now, return empty metadata
	return &FDWMetadata{
		Version:      "1.0",
		Tables:       make(map[string]*FDWTableInfo),
		LastModified: time.Now(),
	}, nil
}
