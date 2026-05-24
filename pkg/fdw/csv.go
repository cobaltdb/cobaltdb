package fdw

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultCSVMaxRows  = 1_000_000
	defaultCSVMaxBytes = 256 << 20 // 256 MiB
)

// CSVWrapper is a ForeignDataWrapper that reads data from CSV files.
type CSVWrapper struct {
	file     *os.File
	maxRows  int
	maxBytes int64
}

type csvCursor struct {
	file       *os.File
	reader     *csv.Reader
	maxRows    int
	returned   int
	pending    []interface{}
	projection []int
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
	if info, err := os.Stat(path); err != nil {
		return err
	} else if maxBytes > 0 && info.Size() > maxBytes {
		return fmt.Errorf("csv FDW file exceeds max_bytes: size=%d max_bytes=%d", info.Size(), maxBytes)
	}
	if c.file != nil {
		if err := c.Close(); err != nil {
			return err
		}
	}
	f, err := os.Open(path) // #nosec G304 - CSV FDW file is an explicit table option and is cleaned before use.
	if err != nil {
		return err
	}
	c.file = f
	c.maxRows = maxRows
	c.maxBytes = maxBytes
	return nil
}

func (c *CSVWrapper) OpenScan(table string, options ScanOptions) (RowCursor, error) {
	if c.file == nil {
		return nil, fmt.Errorf("csv FDW not opened")
	}
	path := c.file.Name()
	if c.maxBytes > 0 {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if info.Size() > c.maxBytes {
			return nil, fmt.Errorf("csv FDW file exceeds max_bytes: size=%d max_bytes=%d", info.Size(), c.maxBytes)
		}
	}

	f, err := os.Open(path) // #nosec G304 - file name was validated in Open before being stored.
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(f)
	first, err := reader.Read()
	if err == io.EOF {
		return &csvCursor{file: f, reader: reader, maxRows: c.maxRows}, nil
	}
	if err != nil {
		f.Close()
		return nil, err
	}

	cursor := &csvCursor{
		file:    f,
		reader:  reader,
		maxRows: c.maxRows,
	}
	if !looksLikeHeader(first) {
		cursor.pending = csvRecordToRow(first)
		return cursor, nil
	}

	cursor.projection = csvProjection(first, options.Columns)
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
		if c.maxRows > 0 && c.returned >= c.maxRows {
			return nil, fmt.Errorf("csv FDW row limit exceeded: max_rows=%d", c.maxRows)
		}
		c.returned++
		return projectCSVRow(csvRecordToRow(record), c.projection), nil
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
