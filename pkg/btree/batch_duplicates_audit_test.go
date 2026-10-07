package btree_test

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/cobaltdb/cobaltdb/pkg/btree"
	"github.com/cobaltdb/cobaltdb/pkg/storage"
	"testing"
)

func verifyBatch(name string, initial string, values []string, want string, wantErr bool) error {
	backend := storage.NewMemory()
	defer backend.Close()
	pool := storage.NewBufferPool(16, backend)
	defer pool.Close()
	tree, err := btree.NewBTreeWithLimit(pool, 32)
	if err != nil {
		return err
	}
	if initial != "" {
		if err := tree.PutString("k", []byte(initial)); err != nil {
			return err
		}
	}
	keys := make([][]byte, len(values))
	vals := make([][]byte, len(values))
	for i, v := range values {
		keys[i] = []byte("k")
		vals[i] = []byte(v)
	}
	err = tree.PutBatch(keys, vals)
	if wantErr {
		if !errors.Is(err, btree.ErrMemoryLimit) {
			return fmt.Errorf("%s error=%v", name, err)
		}
	} else if err != nil {
		return fmt.Errorf("%s: %v", name, err)
	}
	if want != "" {
		got, err := tree.GetString("k")
		if err != nil || !bytes.Equal(got, []byte(want)) {
			return fmt.Errorf("%s value=%q err=%v", name, got, err)
		}
		if tree.Size() != 1 {
			return fmt.Errorf("%s size=%d", name, tree.Size())
		}
	} else if tree.Size() != 0 {
		return fmt.Errorf("%s size=%d", name, tree.Size())
	}
	if tree.MemoryUsed() > 32 {
		return fmt.Errorf("%s memory=%d exceeds 32", name, tree.MemoryUsed())
	}
	if err := tree.Flush(); err != nil {
		return err
	}
	reopened, err := btree.OpenBTreeStrict(pool, tree.RootPageID())
	if err != nil {
		return err
	}
	if reopened.Size() != tree.Size() {
		return fmt.Errorf("%s reopened size mismatch", name)
	}
	if want != "" {
		got, err := reopened.GetString("k")
		if err != nil || string(got) != want {
			return fmt.Errorf("%s reopened value=%q err=%v", name, got, err)
		}
	}
	fmt.Printf("PASS: %s; size=%d memory=%d\n", name, tree.Size(), tree.MemoryUsed())
	return nil
}
func TestAuditBatchDuplicateKeys(t *testing.T) {
	a := string(bytes.Repeat([]byte("a"), 16))
	b := string(bytes.Repeat([]byte("b"), 16))
	boundary := string(bytes.Repeat([]byte("z"), 31))
	large := string(bytes.Repeat([]byte("z"), 39))
	cases := []struct {
		name, initial string
		values        []string
		want          string
		fail          bool
	}{
		{"control", "", []string{a}, a, false},
		{"duplicate insert", "", []string{a, b}, b, false},
		{"shrink at exact limit", boundary, []string{"small", "x"}, "x", false},
		{"grow to exact limit", a, []string{"x", boundary}, boundary, false},
		{"oversized last update", a, []string{"x", large}, a, true},
		{"superseded oversized value", "", []string{large, "x"}, "x", false},
		{"empty", "", nil, "", false},
	}
	for _, c := range cases {
		if err := verifyBatch(c.name, c.initial, c.values, c.want, c.fail); err != nil {
			t.Fatal(err)
		}
	}
}
