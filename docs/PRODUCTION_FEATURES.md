# CobaltDB Production Features

## Overview

CobaltDB v2.2.0 includes enterprise-grade production features designed for high-availability, resilience, and observability in production environments.

## Table of Contents

1. [Lifecycle Management](#lifecycle-management)
2. [Circuit Breaker](#circuit-breaker)
3. [Retry Logic](#retry-logic)
4. [Rate Limiter](#rate-limiter)
5. [SQL Injection Protection](#sql-injection-protection)
6. [Distributed Tracing](#distributed-tracing)
7. [Alerting System](#alerting-system)
8. [Health Checks](#health-checks)
9. [Production Server](#production-server)

---

## Lifecycle Management

**File:** `pkg/server/lifecycle.go`

### Features

- **Graceful Shutdown**: Clean shutdown with configurable timeouts
- **Signal Handling**: Automatic handling of SIGTERM and SIGINT
- **Component Management**: Lifecycle-aware components with start/stop/health hooks
- **State Tracking**: States include Initializing, Starting, Running, Draining, ShuttingDown, Stopped
- **Health Monitoring**: Periodic health checks of all components

### Configuration

```go
config := &server.LifecycleConfig{
    ShutdownTimeout:      30 * time.Second,  // Max time for graceful shutdown
    DrainTimeout:         10 * time.Second,  // Time to wait for connections to drain
    HealthCheckInterval:  5 * time.Second,   // Health check frequency
    StartupTimeout:       60 * time.Second,  // Max startup time
    EnableSignalHandling: true,              // Enable signal handling
}
```

### Usage

```go
lifecycle := server.NewLifecycle(config)
lifecycle.RegisterComponent(myComponent)

if err := lifecycle.Start(); err != nil {
    log.Fatal(err)
}

// Wait for shutdown signal
lifecycle.Wait()
```

---

## Circuit Breaker

**File:** `pkg/engine/circuit_breaker.go`

### Features

- **Three States**: Closed (normal), Open (failing), Half-Open (testing)
- **Automatic Recovery**: Transitions to half-open after timeout
- **Concurrency Control**: Limits concurrent requests per breaker
- **Half-Open Rate Limiting**: Configurable requests during recovery
- **Thread-Safe**: Atomic operations for high concurrency

### Configuration

```go
config := &engine.CircuitBreakerConfig{
    MaxFailures:         5,                 // Failures before opening
    MinSuccesses:        3,                 // Successes to close from half-open
    ResetTimeout:        30 * time.Second,  // Time before attempting reset
    MaxConcurrency:      100,               // Max concurrent requests
    HalfOpenMaxRequests: 1,                 // Requests allowed in half-open
}
```

### Usage

```go
// Single circuit breaker
cb := engine.NewCircuitBreaker(config)

if err := cb.Allow(); err != nil {
    return err // Circuit open
}
defer cb.Release()

// Execute operation
err := doOperation()
if err != nil {
    cb.ReportFailure()
} else {
    cb.ReportSuccess()
}

// Or use Execute wrapper
cb.Execute(ctx, func() error {
    return doOperation()
})

// Multiple circuit breakers
manager := engine.NewCircuitBreakerManager()
cb := manager.GetOrCreate("service1", config)
```

---

## Retry Logic

**File:** `pkg/engine/retry.go`

### Features

- **Exponential Backoff**: Configurable multiplier and jitter
- **Context Cancellation**: Respects context cancellation
- **Retryable Errors**: Whitelist/blacklist specific errors
- **Multiple Policies**: Fast, Standard, Aggressive, Background
- **Result Types**: Generic support for typed results

### Configuration

```go
config := &engine.RetryConfig{
    MaxAttempts:        3,
    InitialDelay:       100 * time.Millisecond,
    MaxDelay:           30 * time.Second,
    Multiplier:         2.0,
    Jitter:             0.1,  // 10% randomization
    RetryableErrors:    []error{io.ErrUnexpectedEOF},
    NonRetryableErrors: []error{context.DeadlineExceeded},
}
```

### Usage

```go
// Basic retry
err := engine.Retry(ctx, config, func() error {
    return db.Query("SELECT * FROM users")
})

// Retry with result
result, err := engine.RetryWithResult(ctx, config, func() (string, error) {
    return fetchData()
})

// Predefined policies
config := engine.GetRetryConfig(engine.RetryPolicyAggressive)
```

### Retry Policies

| Policy | Max Attempts | Initial Delay | Use Case |
|--------|-------------|---------------|----------|
| Fast | 2 | 50ms | Low latency operations |
| Standard | 3 | 100ms | Balanced |
| Aggressive | 5 | 100ms | Critical operations |
| Background | 10 | 1s | Non-urgent operations |

---

## Health Checks

**File:** `pkg/server/lifecycle.go` (Health methods)

### Features

- **Liveness Probe**: `/health` - Kubernetes liveness check
- **Readiness Probe**: `/ready` - Kubernetes readiness check
- **Detailed Health**: `/healthz` - Full health status
- **Circuit Breaker Stats**: `/circuit-breakers` - Breaker status
- **Graceful Shutdown**: `/shutdown` - Trigger graceful shutdown

### Endpoints

```
GET  /health          - Liveness probe (200 = alive, 503 = stopped)
GET  /ready           - Readiness probe (200 = running, 503 = not ready)
GET  /healthz         - Detailed health status (JSON)
GET  /circuit-breakers - Circuit breaker statistics
POST /shutdown        - Trigger graceful shutdown
```

### Response Examples

**Liveness:**
```json
{"alive":true}
```

**Readiness:**
```json
{"ready":true}
```

**Detailed Health:**
```json
{
  "state": "running",
  "healthy": true,
  "components": {
    "database": {"healthy": true, "message": "database healthy"},
    "wire-server": {"healthy": true, "message": "wire server running"}
  }
}
```

---

## Production Server

**File:** `pkg/server/production.go`

### Features

Combines all production features into a single integrated server:

- Lifecycle management
- Circuit breaker protection
- Retry logic
- Health check HTTP server
- Component registration

### Usage

```go
db, _ := engine.Open("cobalt.cb", opts)

config := &server.ProductionConfig{
    Lifecycle: &server.LifecycleConfig{
        ShutdownTimeout: 30 * time.Second,
        DrainTimeout:    10 * time.Second,
    },
    CircuitBreaker:       engine.DefaultCircuitBreakerConfig(),
    Retry:                engine.DefaultRetryConfig(),
    HealthAddr:           ":8420",
    EnableCircuitBreaker: true,
    EnableRetry:          true,
    EnableHealthServer:   true,
}

ps := server.NewProductionServer(db, config)

// Start server
if err := ps.Start(); err != nil {
    log.Fatal(err)
}

// Wait for shutdown
ps.Wait()
```

### Command-Line Flags

The `cobaltdb-server` supports these production flags:

```
--health-addr string        Health check HTTP address (default ":8420")
--health-server             Enable health check HTTP server (default true)
--circuit-breaker           Enable circuit breaker (default true)
--retry                     Enable retry logic (default true)
--shutdown-timeout duration Graceful shutdown timeout (default 30s)
--drain-timeout duration    Connection drain timeout (default 10s)
```

---

## Testing

All production features have comprehensive test coverage:

```bash
# Circuit breaker tests
go test ./pkg/engine -v -run Circuit

# Retry tests
go test ./pkg/engine -v -run Retry

# Lifecycle tests
go test ./pkg/server -v -run Lifecycle

# Production server tests
go test ./pkg/server -v -run Production

# All tests
go test ./...
```

### Test Coverage

- Circuit Breaker: State transitions, concurrency limits, manager operations
- Retry Logic: Success/failure scenarios, context cancellation, backoff calculation
- Lifecycle: Component lifecycle, state hooks, health checks, graceful shutdown
- Production Server: Integration of all features

---

## Best Practices

### Circuit Breaker

1. **Use separate breakers** for different external services
2. **Set appropriate thresholds** based on service reliability
3. **Monitor breaker state** via the `/circuit-breakers` endpoint
4. **Log state transitions** for debugging

### Retry Logic

1. **Use jitter** to prevent thundering herd
2. **Set max delay** to avoid excessive backoff
3. **Whitelist retryable errors** when possible
4. **Respect context cancellation** in operations

### Lifecycle Management

1. **Register all components** for proper cleanup
2. **Set appropriate timeouts** for your deployment
3. **Handle shutdown gracefully** - don't force kill
4. **Use health checks** for load balancer integration

### Monitoring

```bash
# Check liveness
curl http://localhost:8420/health

# Check readiness
curl http://localhost:8420/ready

# Get detailed health
curl http://localhost:8420/healthz

# View circuit breaker stats
curl http://localhost:8420/circuit-breakers

# Trigger graceful shutdown
curl -X POST http://localhost:8420/shutdown
```

---

## Integration with Kubernetes

### Liveness Probe

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8420
  initialDelaySeconds: 10
  periodSeconds: 10
```

### Readiness Probe

```yaml
readinessProbe:
  httpGet:
    path: /ready
    port: 8420
  initialDelaySeconds: 5
  periodSeconds: 5
```

### Graceful Shutdown

```yaml
lifecycle:
  preStop:
    exec:
      command: ["/bin/sh", "-c", "curl -X POST localhost:8420/shutdown"]
terminationGracePeriodSeconds: 60
```

---

## Summary

CobaltDB's production features provide:

- **Resilience**: Circuit breakers prevent cascading failures
- **Reliability**: Retry logic handles transient failures
- **Traffic Control**: Rate limiting prevents overload
- **Security**: SQL injection detection and prevention
- **Observability**: Distributed tracing and health checks
- **Operations**: Alerting system for proactive monitoring
- **Graceful Operations**: Lifecycle management ensures clean startup/shutdown
- **Enterprise Ready**: Kubernetes-compatible probes and graceful shutdown

These features make CobaltDB suitable for production deployments with high availability requirements.
