// Distributed Tracing and Request Context Management
// Provides request correlation and performance tracking

package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RequestIDKey is the context key for request ID
type RequestIDKey struct{}

// RequestContext holds request-scoped information
type RequestContext struct {
	ID            string
	StartTime     time.Time
	ClientID      string
	Operation     string
	SQL           string
	Metadata      map[string]string
	mu            sync.RWMutex
}

// NewRequestContext creates a new request context
func NewRequestContext() *RequestContext {
	return &RequestContext{
		ID:        generateRequestID(),
		StartTime: time.Now(),
		Metadata:  make(map[string]string),
	}
}

// SetMetadata sets a metadata value
func (rc *RequestContext) SetMetadata(key, value string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.Metadata[key] = value
}

// GetMetadata gets a metadata value
func (rc *RequestContext) GetMetadata(key string) string {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.Metadata[key]
}

// Duration returns the request duration
func (rc *RequestContext) Duration() time.Duration {
	return time.Since(rc.StartTime)
}

// ContextWithRequestID adds a request ID to context
func ContextWithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, RequestIDKey{}, requestID)
}

// RequestIDFromContext extracts request ID from context
func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(RequestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// generateRequestID generates a unique request ID
func generateRequestID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b) + fmt.Sprintf("-%d", time.Now().UnixNano())
}

// Tracer manages distributed tracing
type Tracer struct {
	// Sampling rate (0.0-1.0)
	sampleRate float64

	// Spans being traced
	activeSpans sync.Map

	// Completed spans
	completedSpans chan *Span

	// Span handler (e.g., send to Jaeger/Zipkin)
	spanHandler func(*Span)

	// Statistics
	stats TracerStats
}

// Span represents a trace span
type Span struct {
	mu         sync.Mutex
	TraceID    string
	SpanID     string
	ParentID   string
	Operation  string
	StartTime  time.Time
	EndTime    time.Time
	Duration   time.Duration
	Tags       map[string]string
	Logs       []SpanLog
	IsSampled  bool
}

// SpanLog represents a log entry within a span
type SpanLog struct {
	Timestamp time.Time
	Fields    map[string]string
}

// TracerStats holds tracer statistics
type TracerStats struct {
	ActiveSpans    atomic.Int64
	CompletedSpans atomic.Int64
	DroppedSpans   atomic.Int64
	SampledSpans   atomic.Int64
}

// NewTracer creates a new tracer
func NewTracer(sampleRate float64, spanHandler func(*Span)) *Tracer {
	if sampleRate < 0 {
		sampleRate = 0
	}
	if sampleRate > 1 {
		sampleRate = 1
	}

	return &Tracer{
		sampleRate:     sampleRate,
		completedSpans: make(chan *Span, 10000),
		spanHandler:    spanHandler,
	}
}

// StartSpan starts a new span
func (t *Tracer) StartSpan(operation, traceID, parentID string) *Span {
	isSampled := t.shouldSample()

	span := &Span{
		TraceID:   traceID,
		SpanID:    generateSpanID(),
		ParentID:  parentID,
		Operation: operation,
		StartTime: time.Now(),
		Tags:      make(map[string]string),
		IsSampled: isSampled,
	}

	if isSampled {
		t.activeSpans.Store(span.SpanID, span)
		t.stats.ActiveSpans.Add(1)
	}

	return span
}

// FinishSpan finishes a span
func (t *Tracer) FinishSpan(span *Span) {
	if !span.IsSampled {
		return
	}

	span.EndTime = time.Now()
	span.Duration = span.EndTime.Sub(span.StartTime)

	t.activeSpans.Delete(span.SpanID)
	t.stats.ActiveSpans.Add(-1)
	t.stats.CompletedSpans.Add(1)

	// Try to send to handler
	select {
	case t.completedSpans <- span:
	default:
		t.stats.DroppedSpans.Add(1)
	}
}

// AddTag adds a tag to a span
func (t *Tracer) AddTag(span *Span, key, value string) {
	if span == nil || !span.IsSampled {
		return
	}
	span.mu.Lock()
	span.Tags[key] = value
	span.mu.Unlock()
}

// Log adds a log entry to a span
func (t *Tracer) Log(span *Span, fields map[string]string) {
	if span == nil || !span.IsSampled {
		return
	}
	span.mu.Lock()
	span.Logs = append(span.Logs, SpanLog{
		Timestamp: time.Now(),
		Fields:    fields,
	})
	span.mu.Unlock()
}

// shouldSample determines if this request should be sampled
func (t *Tracer) shouldSample() bool {
	if t.sampleRate >= 1.0 {
		return true
	}
	if t.sampleRate <= 0.0 {
		return false
	}

	// Simple random sampling
	b := make([]byte, 1)
	rand.Read(b)
	return float64(b[0])/255.0 < t.sampleRate
}

// GetStats returns tracer statistics (snapshot of current values)
func (t *Tracer) GetStats() TracerStatsSnapshot {
	return TracerStatsSnapshot{
		ActiveSpans:    t.stats.ActiveSpans.Load(),
		CompletedSpans: t.stats.CompletedSpans.Load(),
		DroppedSpans:   t.stats.DroppedSpans.Load(),
		SampledSpans:   t.stats.SampledSpans.Load(),
	}
}

// TracerStatsSnapshot holds a point-in-time snapshot of tracer statistics
type TracerStatsSnapshot struct {
	ActiveSpans    int64 `json:"active_spans"`
	CompletedSpans int64 `json:"completed_spans"`
	DroppedSpans   int64 `json:"dropped_spans"`
	SampledSpans   int64 `json:"sampled_spans"`
}

// generateSpanID generates a unique span ID
func generateSpanID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// RequestTracker tracks active requests for monitoring
type RequestTracker struct {
	activeRequests sync.Map
	totalRequests  atomic.Uint64
	errorRequests  atomic.Uint64
	slowRequests   atomic.Uint64 // > 1 second
}

// TrackedRequest represents an active request
type TrackedRequest struct {
	ID         string
	ClientAddr string
	Operation  string
	SQL        string
	StartTime  time.Time
}

// NewRequestTracker creates a new request tracker
func NewRequestTracker() *RequestTracker {
	return &RequestTracker{}
}

// StartRequest tracks a new request
func (rt *RequestTracker) StartRequest(id, clientAddr, operation string) *TrackedRequest {
	req := &TrackedRequest{
		ID:         id,
		ClientAddr: clientAddr,
		Operation:  operation,
		StartTime:  time.Now(),
	}
	rt.activeRequests.Store(id, req)
	rt.totalRequests.Add(1)
	return req
}

// EndRequest removes a request from tracking
func (rt *RequestTracker) EndRequest(id string, err error) {
	req, ok := rt.activeRequests.LoadAndDelete(id)
	if !ok {
		return
	}

	tracked := req.(*TrackedRequest)
	duration := time.Since(tracked.StartTime)

	if err != nil {
		rt.errorRequests.Add(1)
	}
	if duration > time.Second {
		rt.slowRequests.Add(1)
	}
}

// GetActiveRequests returns currently active requests
func (rt *RequestTracker) GetActiveRequests() []*TrackedRequest {
	var requests []*TrackedRequest
	rt.activeRequests.Range(func(key, value interface{}) bool {
		requests = append(requests, value.(*TrackedRequest))
		return true
	})
	return requests
}

// GetStats returns request statistics
func (rt *RequestTracker) GetStats() RequestTrackerStats {
	count := 0
	rt.activeRequests.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	return RequestTrackerStats{
		ActiveRequests: uint64(count),
		TotalRequests:  rt.totalRequests.Load(),
		ErrorRequests:  rt.errorRequests.Load(),
		SlowRequests:   rt.slowRequests.Load(),
	}
}

// RequestTrackerStats holds request statistics
type RequestTrackerStats struct {
	ActiveRequests uint64 `json:"active_requests"`
	TotalRequests  uint64 `json:"total_requests"`
	ErrorRequests  uint64 `json:"error_requests"`
	SlowRequests   uint64 `json:"slow_requests"`
}

// CorrelationID manages correlation IDs for distributed systems
type CorrelationID struct {
	value string
}

// NewCorrelationID creates a new correlation ID
func NewCorrelationID() *CorrelationID {
	return &CorrelationID{value: generateRequestID()}
}

// String returns the correlation ID string
func (c *CorrelationID) String() string {
	return c.value
}

// correlationIDKey is the context key for correlation IDs
type correlationIDKey struct{}

// ContextWithCorrelationID adds correlation ID to context
func ContextWithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, correlationIDKey{}, id)
}

// CorrelationIDFromContext extracts correlation ID from context
func CorrelationIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(correlationIDKey{}).(string); ok {
		return id
	}
	return ""
}
