package cobaltdb

import (
	"context"
	"testing"
)

func TestConnectorReusesSharedDB(t *testing.T) {
	d := &Driver{}
	c, err := d.OpenConnector("database=:memory:")
	if err != nil {
		t.Fatalf("OpenConnector failed: %v", err)
	}

	conn1Raw, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("first Connect failed: %v", err)
	}
	conn2Raw, err := c.Connect(context.Background())
	if err != nil {
		t.Fatalf("second Connect failed: %v", err)
	}

	conn1, ok := conn1Raw.(*conn)
	if !ok {
		t.Fatalf("unexpected conn type: %T", conn1Raw)
	}
	conn2, ok := conn2Raw.(*conn)
	if !ok {
		t.Fatalf("unexpected conn type: %T", conn2Raw)
	}

	if conn1.db != conn2.db {
		t.Fatal("expected shared DB instance across connections")
	}

	if err := conn1.Close(); err != nil {
		t.Fatalf("first close failed: %v", err)
	}
	if conn2.db.closed {
		t.Fatal("shared DB closed too early while another connection was active")
	}

	if err := conn2.Close(); err != nil {
		t.Fatalf("second close failed: %v", err)
	}
}
