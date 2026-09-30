// Package obs contains technical, business-logic-free helpers shared by every
// service: structured logging, correlation-ID propagation, outbound-call
// latency logging, health endpoints and env parsing.
//
// It is deliberately NOT a place for domain code (see README "Shared code").
//
// AI-assisted (Claude Code): see AI_USAGE.md. Reviewed and understood by the team.
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

// InjectHeaders copies the correlation ID of ctx into a message-header map
// (used for Kafka records).
func InjectHeaders(ctx context.Context, h map[string]string) {
	if id := CorrelationID(ctx); id != "" {
		h[KafkaHeaderCorrelationID] = id
	}
}

// ExtractHeaders returns ctx carrying the correlation ID found in h (or a new
// one when the message carries none).
func ExtractHeaders(ctx context.Context, h map[string]string) context.Context {
	if id := h[KafkaHeaderCorrelationID]; id != "" {
		return WithCorrelationID(ctx, id)
	}
	ctx, _ = EnsureCorrelationID(ctx)
	return ctx
}
