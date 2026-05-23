package catalog

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func catalogDataFilePath(dir, fileName string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", fmt.Errorf("directory cannot be empty")
	}
	if fileName == "" || fileName == "." || fileName == ".." || filepath.IsAbs(fileName) || filepath.Base(fileName) != fileName {
		return "", fmt.Errorf("invalid data file name: %q", fileName)
	}

	cleanDir := filepath.Clean(dir)
	path := filepath.Join(cleanDir, fileName)
	rel, err := filepath.Rel(cleanDir, path)
	if err != nil {
		return "", fmt.Errorf("validate data file path: %w", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("data file path escapes directory: %s", fileName)
	}
	return path, nil
}
