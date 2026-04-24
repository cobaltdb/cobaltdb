package fdw

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
)

// CSVWrapper is a ForeignDataWrapper that reads data from CSV files.
type CSVWrapper struct {
	file *os.File
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
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	c.file = f
	return nil
}

// Scan reads all rows from the CSV file.
// The returned rows contain string values; the query engine handles type coercion.
func (c *CSVWrapper) Scan(table string, columns []string) ([][]interface{}, error) {
	if c.file == nil {
		return nil, fmt.Errorf("csv FDW not opened")
	}
	// Re-open for each scan so multiple scans work
	path := c.file.Name()
	c.file.Close()
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, nil
	}

	// Skip header row if present
	start := 0
	if len(records) > 0 {
		// Heuristic: if first row contains non-numeric values, treat as header
		hasHeader := false
		for _, cell := range records[0] {
			if _, err := strconv.ParseFloat(cell, 64); err != nil {
				hasHeader = true
				break
			}
		}
		if hasHeader {
			start = 1
		}
	}

	var rows [][]interface{}
	for i := start; i < len(records); i++ {
		row := make([]interface{}, len(records[i]))
		for j, cell := range records[i] {
			row[j] = cell
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Close closes the CSV file handle.
func (c *CSVWrapper) Close() error {
	if c.file != nil {
		return c.file.Close()
	}
	return nil
}
