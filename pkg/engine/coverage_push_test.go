package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/query"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
)

func TestRetry_NilConfig(t *testing.T) {
	called := 0
	err := Retry(context.Background(), nil, func() error {
		called++
		return nil
	})
	if err != nil {
		t.Fatalf("Retry nil config: %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestRetry_ContextAlreadyDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := Retry(ctx, DefaultRetryConfig(), func() error {
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetryWithResult_NilConfig(t *testing.T) {
	result, err := RetryWithResult(context.Background(), nil, func() (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("RetryWithResult nil config: %v", err)
	}
	if result != "ok" {
		t.Errorf("expected 'ok', got %q", result)
	}
}

func TestRetryWithResult_ContextAlreadyDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := RetryWithResult(ctx, DefaultRetryConfig(), func() (string, error) {
		return "ok", nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if result != "" {
		t.Errorf("expected empty result, got %q", result)
	}
}

func TestRetryWithResult_NonRetryable(t *testing.T) {
	errFatal := errors.New("fatal")
	cfg := DefaultRetryConfig()
	cfg.NonRetryableErrors = []error{errFatal}
	called := 0

	_, err := RetryWithResult(context.Background(), cfg, func() (string, error) {
		called++
		return "", errFatal
	})
	if called != 1 {
		t.Errorf("expected 1 call (non-retryable), got %d", called)
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestHealthCheck_ClosedDB(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	if err := db.HealthCheck(); err != ErrDatabaseClosed {
		t.Errorf("expected ErrDatabaseClosed, got %v", err)
	}
}

func TestHealthCheck_OpenDB(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.HealthCheck(); err != nil {
		t.Errorf("open DB health check: %v", err)
	}
	if !db.IsHealthy() {
		t.Error("IsHealthy should be true")
	}
}

func TestCreateBackup_NoManager(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.CreateBackup(context.Background(), "full")
	if err == nil {
		t.Error("expected error for in-memory backup")
	}
}

func TestEnablePlanCache_AlreadyEnabled(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.EnablePlanCache(1024, 100)
	db.EnablePlanCache(2048, 200)
}

func TestEnablePlanCache_ZeroDefaults(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db.EnablePlanCache(0, 0)
}

func TestToUpperFast(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello", "HELLO"},
		{"HELLO", "HELLO"},
		{"Hello World", "HELLO WORLD"},
		{"", ""},
		{"123abc", "123ABC"},
	}
	for _, tt := range tests {
		got := toUpperFast(tt.input)
		if got != tt.want {
			t.Errorf("toUpperFast(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestWarmCache_WithInvalidQuery(t *testing.T) {
	cache := NewQueryPlanCache(1024, 100)

	err := cache.WarmCache([]string{
		"SELECT 1",
		"INVALID SQL !!!",
		"SELECT 2",
	})
	if err != nil {
		t.Fatalf("WarmCache: %v", err)
	}
}

func TestTypeNameOf_AllTypes(t *testing.T) {
	tests := []struct {
		val  interface{}
		want string
	}{
		{"hello", "string"},
		{42, "int"},
		{int64(42), "int64"},
		{3.14, "float64"},
		{true, "bool"},
		{[]byte("data"), "[]byte"},
		{nil, "nil"},
		{uint(1), "uint"},
	}
	for _, tt := range tests {
		got := typeNameOf(tt.val)
		if got != tt.want {
			t.Errorf("typeNameOf(%T) = %q, want %q", tt.val, got, tt.want)
		}
	}
}

func TestBeginHotBackup_ClosedDB(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	err = db.BeginHotBackup()
	if err != ErrDatabaseClosed {
		t.Errorf("expected ErrDatabaseClosed, got %v", err)
	}
}

func TestOpen_MemoryPath(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if db.GetDatabasePath() != ":memory:" {
		t.Errorf("expected :memory: path, got %q", db.GetDatabasePath())
	}
}

func TestExec_SlowQueryLog(t *testing.T) {
	ctx := context.Background()
	opts := DefaultOptions()
	opts.SlowQueryLog.EnableSlowQueryLog = true
	opts.SlowQueryLog.Threshold = 0
	db, err := Open(":memory:", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(ctx, "CREATE TABLE t (id INTEGER)")
	if err != nil {
		t.Fatalf("create table: %v", err)
	}

	_, err = db.Exec(ctx, "INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
}

func TestRetry_CancelDuringDelay(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 500 * time.Millisecond
	cfg.MaxAttempts = 5

	ctx, cancel := context.WithCancel(context.Background())
	called := 0

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := Retry(ctx, cfg, func() error {
		called++
		return errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestRetryWithResult_CancelDuringDelay(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.InitialDelay = 500 * time.Millisecond
	cfg.MaxAttempts = 5

	ctx, cancel := context.WithCancel(context.Background())
	called := 0

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := RetryWithResult(ctx, cfg, func() (string, error) {
		called++
		return "", errors.New("fail")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if called != 1 {
		t.Errorf("expected 1 call, got %d", called)
	}
}

func TestExec_IndexAdvisor(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, _ = db.Exec(ctx, "CREATE TABLE t (id INTEGER, name TEXT)")
	_, _ = db.Exec(ctx, "SELECT * FROM t WHERE name = 'test'")
}

func TestSaveMetaPage_ReadFail(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestIsRetryable_EdgeCases(t *testing.T) {
	cfg := DefaultRetryConfig()

	if cfg.IsRetryable(nil) {
		t.Error("nil error should not be retryable")
	}
	if !cfg.IsRetryable(errors.New("fail")) {
		t.Error("regular error should be retryable")
	}

	errFatal := errors.New("fatal")
	cfg.NonRetryableErrors = []error{errFatal}
	if cfg.IsRetryable(errFatal) {
		t.Error("non-retryable error should not be retryable")
	}

	errTransient := errors.New("transient")
	cfg2 := DefaultRetryConfig()
	cfg2.RetryableErrors = []error{errTransient}
	if cfg2.IsRetryable(errors.New("other")) {
		t.Error("non-listed error should not be retryable with retryable-only list")
	}
	if !cfg2.IsRetryable(errTransient) {
		t.Error("listed error should be retryable")
	}
}

func TestWarmCache_EmptyList(t *testing.T) {
	cache := NewQueryPlanCache(1024, 100)

	err := cache.WarmCache(nil)
	if err != nil {
		t.Fatalf("WarmCache nil: %v", err)
	}
}

func TestQueryPlanCache_HashWithArgs(t *testing.T) {
	cache := NewQueryPlanCache(1024, 100)

	sql := "SELECT ?"
	args := []interface{}{"hello", 42, int64(100), 3.14, true, []byte("data"), nil}

	stmt, _ := query.Parse("SELECT 1")
	_ = cache.Put(sql, args, stmt)

	got, ok := cache.Get(sql, args)
	if !ok {
		t.Fatal("expected cache hit")
	}
	_ = got
}

func TestEvictLRU_EmptyCache(t *testing.T) {
	cache := NewQueryPlanCache(1024, 100)
	cache.evictLRU()
}

func TestRetry_MaxAttemptsExhausted(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.MaxAttempts = 2
	cfg.InitialDelay = time.Millisecond

	called := 0
	err := Retry(context.Background(), cfg, func() error {
		called++
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if called != 2 {
		t.Errorf("expected 2 calls, got %d", called)
	}
}

func TestRetryWithResult_MaxAttemptsExhausted(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.MaxAttempts = 2
	cfg.InitialDelay = time.Millisecond

	called := 0
	result, err := RetryWithResult(context.Background(), cfg, func() (int, error) {
		called++
		return 0, errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if result != 0 {
		t.Errorf("expected zero result, got %d", result)
	}
	if called != 2 {
		t.Errorf("expected 2 calls, got %d", called)
	}
}

func TestRetry_SuccessOnSecondAttempt(t *testing.T) {
	cfg := DefaultRetryConfig()
	cfg.MaxAttempts = 3
	cfg.InitialDelay = time.Millisecond

	called := 0
	err := Retry(context.Background(), cfg, func() error {
		called++
		if called < 2 {
			return errors.New("fail")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success: %v", err)
	}
	if called != 2 {
		t.Errorf("expected 2 calls, got %d", called)
	}
}

func TestOpen_DiskWithEncryption(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.EncryptionConfig = &storage.EncryptionConfig{
		Enabled: true,
		Key:     []byte(strings.Repeat("a", 32)),
	}
	db, err := Open(dir+"/test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestOpen_DiskWithCompression(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.PageCompression.Config = &storage.CompressionConfig{
		Enabled: true,
	}
	db, err := Open(dir+"/test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestOpen_InvalidPath(t *testing.T) {
	_, err := Open("/dev/null/impossible/path/test.db", nil)
	if err == nil {
		t.Error("expected error for invalid path")
	}
}

func TestExec_AfterClose(t *testing.T) {
	ctx := context.Background()
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	_, err = db.Exec(ctx, "SELECT 1")
	if err == nil {
		t.Error("expected error after close")
	}
}

func TestGetIndexRecommendations_NoAdvisor(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	recs := db.GetIndexRecommendations()
	_ = recs
}

func TestGetStats_OpenDB(t *testing.T) {
	db, err := Open(":memory:", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stats, err := db.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats == nil {
		t.Error("expected stats")
	}
}
