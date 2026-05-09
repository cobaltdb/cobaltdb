package catalog

import (
	"runtime"

	"github.com/petermattis/goid"
)

// goroutineID returns the current goroutine ID without calling
// runtime.Stack, which is extremely slow and serializes goroutines
// internally.  We use github.com/petermattis/goid which reads the
// goid field directly from the runtime g struct via architecture-
// specific assembly.
func goroutineID() int64 {
	return goid.Get()
}

// init verifies the goroutineID function returns the same value as
// runtime.Stack on this Go version / architecture.  If the assembly
// is wrong the binary panics immediately on startup so we catch it
// in tests rather than silently corrupting the goroutine-to-txn map.
func init() {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	var fromStack int64
	for i := 10; i < n; i++ {
		c := buf[i]
		if c < '0' || c > '9' {
			break
		}
		fromStack = fromStack*10 + int64(c-'0')
	}
	if goroutineID() != fromStack {
		panic("catalog: goroutineID mismatch; goid assembly is broken on this Go version/architecture")
	}
}
