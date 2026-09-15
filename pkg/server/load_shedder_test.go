package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cobaltdb/cobaltdb/pkg/engine"
)

func TestLoadShedderSignals(t *testing.T) {
	t.Run("queue depth", func(t *testing.T) {
		l := NewLoadShedder(nil, nil, nil, 50)
		l.queueDepth = func() int { return 50 }
		l.numGoroutine = func() int { return 1 }
		if err := l.Admit(false); !errors.Is(err, ErrLoadShed) {
			t.Fatalf("Admit error = %v", err)
		}
	})

	t.Run("goroutine pressure", func(t *testing.T) {
		l := NewLoadShedder(nil, nil, nil, 50)
		l.queueDepth = func() int { return 0 }
		l.goroutineLimit = 10
		l.numGoroutine = func() int { return 11 }
		if err := l.Admit(true); !errors.Is(err, ErrLoadShed) {
			t.Fatalf("critical Admit error = %v", err)
		}
	})

	t.Run("half-open probes are not shed", func(t *testing.T) {
		cb := engine.NewCircuitBreaker(&engine.CircuitBreakerConfig{
			MaxFailures: 1, MinSuccesses: 1, ResetTimeout: time.Nanosecond,
			MaxConcurrency: 1, HalfOpenMaxRequests: 1,
		})
		cb.ReportFailure()
		// Drive an immediately-resettable open breaker into half-open without
		// relying on wall-clock sleeps.
		if err := cb.Allow(); err != nil {
			t.Fatal(err)
		}
		cb.Release()
		l := NewLoadShedder(nil, cb, nil, 50)
		l.queueDepth = func() int { return 0 }
		l.numGoroutine = func() int { return 1 }
		l.goroutineLimit = 10
		// Shedding on half-open would starve the breaker of the sequential
		// successful probes it needs to reach MinSuccesses (a recovery
		// deadlock), so admission must pass; the breaker's own token gate
		// bounds probe concurrency.
		if err := l.Admit(false); err != nil {
			t.Fatalf("non-critical Admit error = %v", err)
		}
		if err := l.Admit(true); err != nil {
			t.Fatalf("critical Admit error = %v", err)
		}
	})
}

func TestProductionLoadShedHTTPReturns503(t *testing.T) {
	ps := NewProductionServer(nil, DefaultProductionConfig())
	ps.LoadShedder.queueDepth = func() int { return defaultLoadShedQueueDepth }
	ps.LoadShedder.numGoroutine = func() int { return 1 }
	rr := httptest.NewRecorder()
	ps.loadShedHTTPHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("overloaded request reached handler")
	})).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rr.Code != http.StatusServiceUnavailable || rr.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d retry-after=%q", rr.Code, rr.Header().Get("Retry-After"))
	}
}
