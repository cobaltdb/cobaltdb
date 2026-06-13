package catalog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxCatalogDataFileBytes = 64 << 20

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

func prepareCatalogDataDir(dir string, create bool) error {
	cleanDir, err := cleanCatalogDataDir(dir)
	if err != nil {
		return err
	}
	if err := rejectCatalogSymlinkPathComponents(cleanDir); err != nil {
		return err
	}

	info, statErr := os.Lstat(cleanDir)
	preexisting := statErr == nil
	if statErr != nil {
		if os.IsNotExist(statErr) && !create {
			return statErr
		}
		if !os.IsNotExist(statErr) {
			return fmt.Errorf("failed to stat catalog data directory: %w", statErr)
		}
	}
	if preexisting {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("catalog data directory must not be a symlink: %s", cleanDir)
		}
		if !info.IsDir() {
			return fmt.Errorf("catalog data directory must be a directory: %s", cleanDir)
		}
	}

	if create {
		if err := os.MkdirAll(cleanDir, 0750); err != nil {
			return err
		}
		if err := os.Chmod(cleanDir, 0750); err != nil {
			return err
		}
	}

	openedInfo, err := os.Stat(cleanDir)
	if err != nil {
		return err
	}
	if !openedInfo.IsDir() {
		return fmt.Errorf("catalog data directory must be a directory: %s", cleanDir)
	}
	if preexisting && !os.SameFile(info, openedInfo) {
		return fmt.Errorf("catalog data directory changed while opening: %s", cleanDir)
	}
	return nil
}

func cleanCatalogDataDir(dir string) (string, error) {
	if strings.TrimSpace(dir) == "" {
		return "", fmt.Errorf("directory cannot be empty")
	}
	return filepath.Clean(dir), nil
}

func rejectCatalogSymlinkPathComponents(path string) error {
	path = filepath.Clean(path)
	if path == "." || path == string(os.PathSeparator) {
		return nil
	}

	current := "."
	if filepath.IsAbs(path) {
		current = string(os.PathSeparator)
		path = strings.TrimPrefix(path, string(os.PathSeparator))
	}
	for _, part := range strings.Split(path, string(os.PathSeparator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("failed to stat catalog data directory component: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("catalog data directory component must not be a symlink: %s", current)
		}
	}
	return nil
}

func readCatalogDataFile(path string) ([]byte, error) {
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("catalog data file must not be a symlink: %s", path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("catalog data file must be a regular file: %s", path)
	}
	if info.Size() > maxCatalogDataFileBytes {
		return nil, fmt.Errorf("catalog data file is too large: %d bytes (max %d)", info.Size(), maxCatalogDataFileBytes)
	}

	file, err := os.Open(path) // #nosec G304 - path is validated by catalogDataFilePath before use.
	if err != nil {
		return nil, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("catalog data file must be a regular file: %s", path)
	}
	if openedInfo.Size() > maxCatalogDataFileBytes {
		return nil, fmt.Errorf("catalog data file is too large: %d bytes (max %d)", openedInfo.Size(), maxCatalogDataFileBytes)
	}
	if !os.SameFile(info, openedInfo) {
		return nil, fmt.Errorf("catalog data file changed while opening: %s", path)
	}
	if err := file.Chmod(0600); err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(file, maxCatalogDataFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxCatalogDataFileBytes {
		return nil, fmt.Errorf("catalog data file is too large: %d bytes (max %d)", len(data), maxCatalogDataFileBytes)
	}
	return data, nil
}

func writeCatalogDataFileAtomic(path string, data []byte, perm os.FileMode) error {
	path = filepath.Clean(path)
	dir := filepath.Dir(path)
	base := filepath.Base(path)

	file, err := os.CreateTemp(dir, "."+base+".tmp-*") // #nosec G304 - path is validated by catalogDataFilePath before use.
	if err != nil {
		return fmt.Errorf("create temporary data file: %w", err)
	}
	tmpPath := file.Name()
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if tmpPath != "" {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := file.Chmod(perm); err != nil {
		return fmt.Errorf("set temporary data file permissions: %w", err)
	}
	if _, err := writeCatalogDataFull(file, data); err != nil {
		return fmt.Errorf("write temporary data file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync temporary data file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temporary data file: %w", err)
	}
	closed = true

	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace data file: %w", err)
	}
	tmpPath = ""
	if err := syncCatalogDataDir(dir); err != nil {
		return fmt.Errorf("sync data directory: %w", err)
	}
	return nil
}

func writeCatalogDataFull(writer io.Writer, data []byte) (int, error) {
	n, err := writer.Write(data)
	if err != nil {
		return n, err
	}
	if n != len(data) {
		return n, io.ErrShortWrite
	}
	return n, nil
}

func syncCatalogDataDir(dir string) error {
	file, err := os.Open(dir) // #nosec G304 - directory is derived from a validated catalog data path.
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
