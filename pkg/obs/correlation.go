// Package obs contains technical, business-logic-free helpers shared by every
// service: structured logging, correlation-ID propagation, outbound-call
// latency logging, health endpoints and env parsing.
//
// It is not a place for domain code (see README "Shared code").
package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
)

// HeaderCorrelationID is the HTTP header that carries the correlation ID
// across every service in one request flow.
const HeaderCorrelationID = "X-Correlation-ID"

// KafkaHeaderCorrelationID is the message-header key that carries the same ID
// through the broker (producer -> consumers).
const KafkaHeaderCorrelationID = "correlation_id"

// HeaderCaller names the calling instance (e.g. the Aggregator pod). The
// request log records it, so a callee's log shows which replica called it.
const HeaderCaller = "X-Caller"

type ctxKey struct{}

// NewCorrelationID returns a random 16-hex-char identifier.
func NewCorrelationID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// WithCorrelationID stores id in ctx.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// CorrelationID returns the ID stored in ctx, or "" when absent.
func CorrelationID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// EnsureCorrelationID returns ctx carrying a correlation ID (existing or new)
// together with that ID.
func EnsureCorrelationID(ctx context.Context) (context.Context, string) {
	if id := CorrelationID(ctx); id != "" {
		return ctx, id
	}
	id := NewCorrelationID()
	return WithCorrelationID(ctx, id), id
}

// InjectHeaders copies the correlation ID and the trace context of ctx into a
// message-header map (used for Kafka records).
func InjectHeaders(ctx context.Context, h map[string]string) {
	if id := CorrelationID(ctx); id != "" {
		h[KafkaHeaderCorrelationID] = id
	}
	if sc, ok := SpanContextFrom(ctx); ok {
		h[HeaderTraceparent] = sc.Traceparent()
	}
}

// ExtractHeaders returns ctx carrying the correlation ID found in h (or a new
// one when the message carries none).
// The trace context, when present, becomes the parent of spans started from it.
func ExtractHeaders(ctx context.Context, h map[string]string) context.Context {
	if sc, ok := ParseTraceparent(h[HeaderTraceparent]); ok {
		ctx = ContextWithSpanContext(ctx, sc)
	}
	if id := h[KafkaHeaderCorrelationID]; id != "" {
		return WithCorrelationID(ctx, id)
	}
	ctx, _ = EnsureCorrelationID(ctx)
	return ctx
}
