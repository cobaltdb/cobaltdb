package metrics

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPrometheusSlowQueriesCountSurvivesRetentionAndClear(t *testing.T) {
	assertCount := func(t *testing.T, log *SlowQueryLog, want uint64) {
		t.Helper()
		unregister := RegisterSlowQueryLog(log)
		defer unregister()
		recorder := httptest.NewRecorder()
		NewPrometheusMetrics().Handler()(recorder, httptest.NewRequest("GET", "/metrics", nil))
		line := fmt.Sprintf("cobaltdb_slow_queries_total %d\n", want)
		if recorder.Code != 200 || !strings.Contains(recorder.Body.String(), line) {
			t.Fatalf("incorrect cumulative metric: %s", recorder.Body.String())
		}
	}
	for _, capacity := range []int{0, 1, 2, 4} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			log := NewSlowQueryLog(true, time.Millisecond, capacity, "")
			assertCount(t, log, 0)
			for i := 0; i < 3; i++ {
				log.Log("SELECT 1", 2*time.Millisecond, 0, 1)
			}
			assertCount(t, log, 3)
			retained, avg := log.GetStats()
			wantRetained := capacity
			if wantRetained > 3 {
				wantRetained = 3
			}
			if retained != wantRetained || (retained > 0 && avg != 2*time.Millisecond) {
				t.Fatal("retained-entry statistics changed")
			}
			log.Log("fast", time.Microsecond, 0, 0)
			log.Disable()
			log.Log("disabled", time.Second, 0, 0)
			assertCount(t, log, 3)
			log.Clear()
			assertCount(t, log, 3)
			if retained, avg := log.GetStats(); retained != 0 || avg != 0 {
				t.Fatal("Clear did not clear retained entries")
			}
			log.Enable()
			log.Log("after-clear", time.Millisecond, 0, 0)
			assertCount(t, log, 4)
		})
	}
	t.Run("concurrent-recording", func(t *testing.T) {
		log := NewSlowQueryLog(true, time.Millisecond, 2, "")
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for j := 0; j < 100; j++ {
					log.Log("concurrent", time.Millisecond, 0, 0)
				}
			}()
		}
		close(start)
		wg.Wait()
		assertCount(t, log, 800)
		if retained, _ := log.GetStats(); retained != 2 {
			t.Fatal("concurrent retention bound violated")
		}
	})
}
