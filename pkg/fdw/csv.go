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

// Scan reads CSV rows with bounded materialization.
// The returned rows contain string values; the query engine handles type coercion.
func (c *CSVWrapper) Scan(table string, columns []string) ([][]interface{}, error) {
	if c.file == nil {
		return nil, fmt.Errorf("csv FDW not opened")
	}
	// Re-open for each scan so multiple scans work independently
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
	defer f.Close()

	reader := csv.NewReader(f)
	first, err := reader.Read()
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var rows [][]interface{}
	if !looksLikeHeader(first) {
		rows = append(rows, csvRecordToRow(first))
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if c.maxRows > 0 && len(rows) >= c.maxRows {
			return nil, fmt.Errorf("csv FDW row limit exceeded: max_rows=%d", c.maxRows)
		}
		rows = append(rows, csvRecordToRow(record))
	}
	return rows, nil
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
