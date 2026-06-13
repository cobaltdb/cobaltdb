package fdw

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultCSVMaxRows       = 1_000_000
	defaultCSVMaxBytes      = 256 << 20 // 256 MiB
	defaultCSVMaxFieldBytes = 1 << 20   // 1 MiB
	defaultCSVMaxColumns    = 4096
)

// CSVWrapper is a ForeignDataWrapper that reads data from CSV files.
type CSVWrapper struct {
	file          *os.File
	maxRows       int
	maxBytes      int64
	maxFieldBytes int
	maxColumns    int
}

type csvCursor struct {
	file          *os.File
	reader        *csv.Reader
	maxRows       int
	maxFieldBytes int
	maxColumns    int
	returned      int
	pending       []interface{}
	projection    []int
	predicates    []csvPredicate
}

type csvPredicate struct {
	index    int
	operator string
	value    interface{}
}

// Name returns the FDW identifier.
func (c *CSVWrapper) Name() string {
	return "csv"
}

// Open opens the CSV file specified in options["file"].
func (c *CSVWrapper) Open(options map[string]string) error {
	path, ok := options["file"]
	if !ok {
		return fmt.Errorf("csv FDW requires 'file' option")
	}
	path, err := cleanCSVPath(path)
	if err != nil {
		return err
	}
	maxRows, err := parsePositiveIntOption(options, "max_rows", defaultCSVMaxRows)
	if err != nil {
		return err
	}
	maxBytes, err := parsePositiveInt64Option(options, "max_bytes", defaultCSVMaxBytes)
	if err != nil {
		return err
	}
	maxFieldBytes, err := parsePositiveIntOption(options, "max_field_bytes", defaultCSVMaxFieldBytes)
	if err != nil {
		return err
	}
	maxColumns, err := parsePositiveIntOption(options, "max_columns", defaultCSVMaxColumns)
	if err != nil {
		return err
	}
	f, err := openCSVRegularFile(path, maxBytes)
	if err != nil {
		return err
	}
	if c.file != nil {
		if err := c.Close(); err != nil {
			_ = f.Close()
			return err
		}
	}
	c.file = f
	c.maxRows = maxRows
	c.maxBytes = maxBytes
	c.maxFieldBytes = maxFieldBytes
	c.maxColumns = maxColumns
	return nil
}

func (c *CSVWrapper) OpenScan(table string, options ScanOptions) (RowCursor, error) {
	if c.file == nil {
		return nil, fmt.Errorf("csv FDW not opened")
	}
	path := c.file.Name()
	f, err := openCSVRegularFile(path, c.maxBytes)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(f)
	first, err := reader.Read()
	if err == io.EOF {
		return &csvCursor{file: f, reader: reader, maxRows: c.maxRows}, nil
	}
	if err != nil {
		err = errors.Join(err, f.Close())
		return nil, err
	}
	if err := validateCSVRecord(first, c.maxColumns, c.maxFieldBytes); err != nil {
		err = errors.Join(err, f.Close())
		return nil, err
	}

	cursor := &csvCursor{
		file:          f,
		reader:        reader,
		maxRows:       c.maxRows,
		maxFieldBytes: c.maxFieldBytes,
		maxColumns:    c.maxColumns,
	}
	if !looksLikeHeader(first) {
		cursor.pending = csvRecordToRow(first)
		return cursor, nil
	}

	cursor.projection = csvProjection(first, options.Columns)
	cursor.predicates = csvPredicates(first, options.Predicates)
	return cursor, nil
}

// Scan reads CSV rows with bounded materialization.
// The returned rows contain string values; the query engine handles type coercion.
func (c *CSVWrapper) Scan(table string, columns []string) ([][]interface{}, error) {
	cursor, err := c.OpenScan(table, ScanOptions{Columns: columns})
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	var rows [][]interface{}
	for {
		row, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (c *csvCursor) Next() ([]interface{}, error) {
	if c.pending != nil {
		if c.maxRows > 0 && c.returned >= c.maxRows {
			return nil, fmt.Errorf("csv FDW row limit exceeded: max_rows=%d", c.maxRows)
		}
		row := c.pending
		c.pending = nil
		c.returned++
		return projectCSVRow(row, c.projection), nil
	}
	for {
		record, err := c.reader.Read()
		if err == io.EOF {
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		if err := validateCSVRecord(record, c.maxColumns, c.maxFieldBytes); err != nil {
			return nil, err
		}
		row := csvRecordToRow(record)
		if !matchesCSVPredicates(row, c.predicates) {
			continue
		}
		if c.maxRows > 0 && c.returned >= c.maxRows {
			return nil, fmt.Errorf("csv FDW row limit exceeded: max_rows=%d", c.maxRows)
		}
		c.returned++
		return projectCSVRow(row, c.projection), nil
	}
}

func (c *csvCursor) Close() error {
	if c.file == nil {
		return nil
	}
	err := c.file.Close()
	c.file = nil
	return err
}

// Close closes the CSV file handle.
func (c *CSVWrapper) Close() error {
	if c.file != nil {
		err := c.file.Close()
		c.file = nil
		return err
	}
	return nil
}

func cleanCSVPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("csv file path cannot be empty")
	}
	return filepath.Clean(path), nil
}

func openCSVRegularFile(path string, maxBytes int64) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("csv FDW file must not be a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("csv FDW file must be a regular file: %s", path)
	}
	if maxBytes > 0 && info.Size() > maxBytes {
		return nil, fmt.Errorf("csv FDW file exceeds max_bytes: size=%d max_bytes=%d", info.Size(), maxBytes)
	}

	f, err := os.Open(path) // #nosec G304 - CSV FDW file is an explicit table option and is validated before use.
	if err != nil {
		return nil, err
	}
	openedInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("csv FDW file must be a regular file: %s", path)
	}
	if !os.SameFile(info, openedInfo) {
		_ = f.Close()
		return nil, fmt.Errorf("csv FDW file changed while opening: %s", path)
	}
	if maxBytes > 0 && openedInfo.Size() > maxBytes {
		_ = f.Close()
		return nil, fmt.Errorf("csv FDW file exceeds max_bytes: size=%d max_bytes=%d", openedInfo.Size(), maxBytes)
	}
	return f, nil
}

func looksLikeHeader(record []string) bool {
	for _, cell := range record {
		if _, err := strconv.ParseFloat(cell, 64); err != nil {
			return true
		}
	}
	return false
}

func csvRecordToRow(record []string) []interface{} {
	row := make([]interface{}, len(record))
	for j, cell := range record {
		row[j] = cell
	}
	return row
}

func validateCSVRecord(record []string, maxColumns, maxFieldBytes int) error {
	if maxColumns > 0 && len(record) > maxColumns {
		return fmt.Errorf("csv FDW column limit exceeded: columns=%d max_columns=%d", len(record), maxColumns)
	}
	if maxFieldBytes > 0 {
		for i, field := range record {
			if len(field) > maxFieldBytes {
				return fmt.Errorf("csv FDW field exceeds max_field_bytes: column=%d size=%d max_field_bytes=%d", i, len(field), maxFieldBytes)
			}
		}
	}
	return nil
}

func csvProjection(header []string, columns []string) []int {
	if len(columns) == 0 {
		return nil
	}
	indexByName := make(map[string]int, len(header))
	for i, name := range header {
		indexByName[name] = i
	}
	projection := make([]int, 0, len(columns))
	for _, col := range columns {
		idx, ok := indexByName[col]
		if !ok {
			return nil
		}
		projection = append(projection, idx)
	}
	if len(projection) == len(header) {
		for i, idx := range projection {
			if idx != i {
				return projection
			}
		}
		return nil
	}
	return projection
}

func projectCSVRow(row []interface{}, projection []int) []interface{} {
	if len(projection) == 0 {
		return row
	}
	projected := make([]interface{}, 0, len(projection))
	for _, idx := range projection {
		if idx >= 0 && idx < len(row) {
			projected = append(projected, row[idx])
		} else {
			projected = append(projected, nil)
		}
	}
	return projected
}

func csvPredicates(header []string, predicates []Predicate) []csvPredicate {
	if len(predicates) == 0 {
		return nil
	}
	indexByName := make(map[string]int, len(header))
	for i, name := range header {
		indexByName[name] = i
	}
	result := make([]csvPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		idx, ok := indexByName[predicate.Column]
		if !ok {
			continue
		}
		result = append(result, csvPredicate{
			index:    idx,
			operator: predicate.Operator,
			value:    predicate.Value,
		})
	}
	return result
}

func matchesCSVPredicates(row []interface{}, predicates []csvPredicate) bool {
	for _, predicate := range predicates {
		if predicate.index < 0 || predicate.index >= len(row) {
			continue
		}
		if !matchesCSVPredicate(row[predicate.index], predicate) {
			return false
		}
	}
	return true
}

func matchesCSVPredicate(cell interface{}, predicate csvPredicate) bool {
	left := fmt.Sprint(cell)
	if predicate.value == nil {
		switch predicate.operator {
		case "=":
			return left == ""
		case "!=":
			return left != ""
		default:
			return true
		}
	}
	right := fmt.Sprint(predicate.value)
	switch predicate.operator {
	case "=":
		return left == right
	case "!=":
		return left != right
	}

	leftNum, leftErr := strconv.ParseFloat(left, 64)
	rightNum, rightErr := strconv.ParseFloat(right, 64)
	if leftErr != nil || rightErr != nil {
		return true
	}
	switch predicate.operator {
	case "<":
		return leftNum < rightNum
	case ">":
		return leftNum > rightNum
	case "<=":
		return leftNum <= rightNum
	case ">=":
		return leftNum >= rightNum
	default:
		return true
	}
}

func parsePositiveIntOption(options map[string]string, name string, defaultValue int) (int, error) {
	value, ok := options[name]
	if !ok || strings.TrimSpace(value) == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("csv FDW option %s must be a non-negative integer", name)
	}
	return parsed, nil
}

func parsePositiveInt64Option(options map[string]string, name string, defaultValue int64) (int64, error) {
	value, ok := options[name]
	if !ok || strings.TrimSpace(value) == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("csv FDW option %s must be a non-negative integer", name)
	}
	return parsed, nil
}
