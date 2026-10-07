package backup_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/backup"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type database struct {
	path       string
	begin, end int
}

func (d *database) GetDatabasePath() string { return d.path }
func (d *database) GetWALPath() string      { return "" }
func (d *database) Checkpoint() error       { return nil }
func (d *database) BeginHotBackup() error   { d.begin++; return nil }
func (d *database) EndHotBackup() error     { d.end++; return nil }
func (d *database) GetCurrentLSN() uint64   { return 0 }
func verifyMetadata(prior bool) error {
	root, err := os.MkdirTemp("", "backup-verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	source := filepath.Join(root, "source.db")
	original := []byte("test database")
	if err = os.WriteFile(source, original, 0600); err != nil {
		return err
	}
	db := &database{path: source}
	dir := filepath.Join(root, "backups")
	cfg := &backup.Config{BackupDir: dir, Verify: true}
	m := backup.NewManager(cfg, db)
	if err := m.MetadataError(); err != nil {
		return err
	}
	var previous *backup.Backup
	if prior {
		previous, err = m.CreateBackup(context.Background(), backup.TypeFull)
		if err != nil {
			return err
		}
	}
	meta := filepath.Join(dir, "metadata.json")
	saved := filepath.Join(dir, "metadata.saved")
	var injectedErr error
	m.OnProgress = func(int) {
		if prior {
			injectedErr = os.Rename(meta, saved)
			if injectedErr != nil {
				return
			}
		}
		injectedErr = os.Mkdir(meta, 0750)
	}
	result, err := m.CreateBackup(context.Background(), backup.TypeFull)
	if injectedErr != nil {
		return injectedErr
	}
	if err == nil || !strings.Contains(err.Error(), "failed to save backup metadata") || result != nil {
		return fmt.Errorf("save failure not propagated: %v", err)
	}
	wantCount := 0
	if prior {
		wantCount = 1
	}
	records := m.ListBackups()
	if len(records) != wantCount {
		return fmt.Errorf("phantom backup count=%d want=%d", len(records), wantCount)
	}
	if prior && (records[0].ID != previous.ID || m.GetBackup(previous.ID) == nil) {
		return fmt.Errorf("previous backup lost")
	}
	if m.IsBackupInProgress() || db.begin != db.end {
		return fmt.Errorf("hot backup not released")
	}
	fmt.Printf("PASS: injected save failure with prior=%t leaves listed=%d; previous records preserved\n", prior, wantCount)
	if err := os.Remove(meta); err != nil {
		return err
	}
	if prior {
		if err := os.Rename(saved, meta); err != nil {
			return err
		}
	}
	m.OnProgress = nil
	fresh, err := m.CreateBackup(context.Background(), backup.TypeFull)
	if err != nil {
		return err
	}
	if len(m.ListBackups()) != wantCount+1 {
		return fmt.Errorf("retry count mismatch")
	}
	reloaded := backup.NewManager(cfg, db)
	if reloaded.MetadataError() != nil || len(reloaded.ListBackups()) != wantCount+1 {
		return fmt.Errorf("reload mismatch: %v", reloaded.MetadataError())
	}
	target := filepath.Join(root, "restored.db")
	if err := reloaded.Restore(context.Background(), fresh.ID, target); err != nil {
		return err
	}
	got, err := os.ReadFile(target)
	if err != nil || !bytes.Equal(got, original) {
		return fmt.Errorf("restore=%q err=%v", got, err)
	}
	fmt.Printf("PASS: retry, reload and restore with prior=%t\n", prior)
	return nil
}
func TestAuditBackupMetadataFailure(t *testing.T) {
	for _, prior := range []bool{false, true} {
		if err := verifyMetadata(prior); err != nil {
			t.Fatal(err)
		}
	}
}
