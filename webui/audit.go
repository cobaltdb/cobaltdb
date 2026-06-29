package main

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// auditEvent is one recorded action against the Web UI. It deliberately stores
// no token material — only the principal ID/name/role that the middleware
// already resolved.
type auditEvent struct {
	Timestamp   time.Time `json:"ts"`
	PrincipalID string    `json:"principal_id"`
	Principal   string    `json:"principal"`
	Role        Role      `json:"role"`
	RemoteAddr  string    `json:"remote_addr"`
	Method      string    `json:"method"`
	Path        string    `json:"path"`
	QueryClass  string    `json:"query_class,omitempty"`
	SQL         string    `json:"sql,omitempty"`
	Outcome     string    `json:"outcome"` // "allowed", "denied", "error"
	Detail      string    `json:"detail,omitempty"`
}

// auditLog is a concurrency-safe sink that both streams events as JSON lines to
// an io.Writer (stderr by default) and keeps a bounded in-memory ring buffer so
// admins can read recent activity over the API.
type auditLog struct {
	mu     sync.Mutex
	w      io.Writer
	ring   []auditEvent
	size   int
	next   int
	full   bool
	maxSQL int
}

func newAuditLog(w io.Writer, ringSize, maxSQL int) *auditLog {
	if ringSize < 1 {
		ringSize = 1
	}
	return &auditLog{
		w:      w,
		ring:   make([]auditEvent, ringSize),
		size:   ringSize,
		maxSQL: maxSQL,
	}
}

// record appends an event to the ring buffer and writes a JSON line to the sink.
func (a *auditLog) record(ev auditEvent) {
	if a == nil {
		return
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now()
	}
	if a.maxSQL > 0 && len(ev.SQL) > a.maxSQL {
		ev.SQL = ev.SQL[:a.maxSQL] + "…"
	}

	a.mu.Lock()
	a.ring[a.next] = ev
	a.next = (a.next + 1) % a.size
	if a.next == 0 {
		a.full = true
	}
	w := a.w
	a.mu.Unlock()

	if w != nil {
		if line, err := json.Marshal(ev); err == nil {
			line = append(line, '\n')
			_, _ = w.Write(line)
		}
	}
}

// recent returns up to `limit` most-recent events, newest first.
func (a *auditLog) recent(limit int) []auditEvent {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	count := a.size
	if !a.full {
		count = a.next
	}
	if limit > 0 && limit < count {
		count = limit
	}
	out := make([]auditEvent, 0, count)
	// Walk backwards from the most recently written slot.
	idx := a.next - 1
	for i := 0; i < count; i++ {
		if idx < 0 {
			idx += a.size
		}
		out = append(out, a.ring[idx])
		idx--
	}
	return out
}
