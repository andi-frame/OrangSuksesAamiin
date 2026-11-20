package obs

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Distributed tracing without an SDK dependency.
//
//   - Context propagation: W3C Trace Context ("traceparent" header) over HTTP
//     and as a Kafka record header.
//   - Export: OTLP/HTTP with JSON encoding to $OTEL_EXPORTER_OTLP_ENDPOINT
//     (Jaeger all-in-one accepts it on :4318). Unset = spans are created (the
//     trace_id still appears in the logs) but not exported.
//   - Export never blocks a request: spans go to a bounded queue; when it is
//     full they are dropped and counted (obs_trace_spans_dropped_total).
//
// The trace id of a flow that starts without an incoming traceparent is derived
// from its correlation ID. Every component that only knows the cid (the outbox
// relay reads it from the stored entry) therefore lands in the same trace, and
// a cid from the logs is enough to find the trace in Jaeger.

// HeaderTraceparent is the W3C trace context header (HTTP and Kafka).
const HeaderTraceparent = "traceparent"

// SpanKind values follow OTLP.
type SpanKind int

const (
	SpanInternal SpanKind = 1
	SpanServer   SpanKind = 2
	SpanClient   SpanKind = 3
	SpanProducer SpanKind = 4
	SpanConsumer SpanKind = 5
)

// SpanContext identifies a span within a trace.
type SpanContext struct {
	TraceID [16]byte
	SpanID  [8]byte
	Sampled bool
	Remote  bool
}

func (sc SpanContext) Valid() bool { return sc.TraceID != [16]byte{} && sc.SpanID != [8]byte{} }

// Traceparent renders the W3C header value.
func (sc SpanContext) Traceparent() string {
	flags := "00"
	if sc.Sampled {
		flags = "01"
	}
	return "00-" + hex.EncodeToString(sc.TraceID[:]) + "-" + hex.EncodeToString(sc.SpanID[:]) + "-" + flags
}

// ParseTraceparent reads a W3C traceparent header.
func ParseTraceparent(v string) (SpanContext, bool) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return SpanContext{}, false
	}
	var sc SpanContext
	if _, err := hex.Decode(sc.TraceID[:], []byte(parts[1])); err != nil {
		return SpanContext{}, false
	}
	if _, err := hex.Decode(sc.SpanID[:], []byte(parts[2])); err != nil {
		return SpanContext{}, false
	}
	f, err := strconv.ParseUint(parts[3], 16, 8)
	if err != nil || !sc.Valid() {
		return SpanContext{}, false
	}
	sc.Sampled, sc.Remote = f&1 == 1, true
	return sc, true
}

type spanCtxKey struct{}

// ContextWithSpanContext makes sc the parent of spans started from ctx.
func ContextWithSpanContext(ctx context.Context, sc SpanContext) context.Context {
	return context.WithValue(ctx, spanCtxKey{}, sc)
}

// SpanContextFrom returns the current span context, if any.
func SpanContextFrom(ctx context.Context) (SpanContext, bool) {
	sc, ok := ctx.Value(spanCtxKey{}).(SpanContext)
	return sc, ok && sc.Valid()
}

// TraceID returns the hex trace id of ctx ("" when there is none).
func TraceID(ctx context.Context) string {
	if sc, ok := SpanContextFrom(ctx); ok {
		return hex.EncodeToString(sc.TraceID[:])
	}
	return ""
}

// TraceIDForCorrelation is the trace id a flow with that cid gets when no
// traceparent came in.
func TraceIDForCorrelation(cid string) [16]byte {
	sum := sha256.Sum256([]byte("cid:" + cid))
	var id [16]byte
	copy(id[:], sum[:16])
	return id
}

// Span is one timed step of a flow.
type Span struct {
	name   string
	kind   SpanKind
	sc     SpanContext
	parent [8]byte
	start  time.Time
	svc    string

	mu     sync.Mutex
	attrs  map[string]any
	errMsg string
	ended  bool
}

// StartSpan starts a child of the span in ctx (or a new trace) and returns a
// ctx carrying it. attrs are key/value pairs. Always call End.
func StartSpan(ctx context.Context, name string, kind SpanKind, attrs ...any) (context.Context, *Span) {
	s := &Span{name: name, kind: kind, start: time.Now(), svc: serviceName(), attrs: map[string]any{}}
	if parent, ok := SpanContextFrom(ctx); ok {
		s.sc.TraceID, s.parent, s.sc.Sampled = parent.TraceID, parent.SpanID, parent.Sampled
	} else {
		if cid := CorrelationID(ctx); cid != "" {
			s.sc.TraceID = TraceIDForCorrelation(cid)
		} else {
			_, _ = rand.Read(s.sc.TraceID[:])
		}
		s.sc.Sampled = sampled(s.sc.TraceID)
	}
	_, _ = rand.Read(s.sc.SpanID[:])
	if cid := CorrelationID(ctx); cid != "" {
		s.attrs["correlation_id"] = cid
	}
	for i := 0; i+1 < len(attrs); i += 2 {
		if k, ok := attrs[i].(string); ok {
			s.attrs[k] = attrs[i+1]
		}
	}
	return ContextWithSpanContext(ctx, s.sc), s
}

// SpanContext returns the identity of s.
func (s *Span) SpanContext() SpanContext { return s.sc }

// SetAttr adds an attribute.
func (s *Span) SetAttr(key string, v any) {
	s.mu.Lock()
	s.attrs[key] = v
	s.mu.Unlock()
}

// RecordError marks the span failed (nil is ignored).
func (s *Span) RecordError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	s.errMsg = err.Error()
	s.mu.Unlock()
}

// End finishes the span and hands it to the exporter (once).
func (s *Span) End() {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.mu.Unlock()
	if s.sc.Sampled {
		defaultExporter().enqueue(s, time.Now())
	}
}

// ------------------------------------------------------------- sampling ---

var sampleRatio = func() float64 {
	if v, err := strconv.ParseFloat(os.Getenv("OTEL_TRACES_SAMPLER_ARG"), 64); err == nil && v >= 0 && v <= 1 {
		return v
	}
	return 1
}()

// sampled is a deterministic ratio sampler on the trace id, so every service
// takes the same decision for the same trace.
func sampled(id [16]byte) bool {
	if sampleRatio >= 1 {
		return true
	}
	var x uint64
	for _, b := range id[8:] {
		x = x<<8 | uint64(b)
	}
	return float64(x>>11)/float64(uint64(1)<<53) < sampleRatio
}

// ------------------------------------------------------------- exporter ---

var (
	spansExported = NewCounter("obs_trace_spans_exported_total", "Spans sent to the trace collector.", "service")
	spansDropped  = NewCounter("obs_trace_spans_dropped_total", "Spans dropped (queue full or collector unreachable).", "service")
)

type finished struct {
	s   *Span
	end time.Time
}

type exporter struct {
	endpoint string
	queue    chan finished
	client   *http.Client
	started  atomic.Bool
}

var (
	expOnce sync.Once
	exp     *exporter
)

func defaultExporter() *exporter {
	expOnce.Do(func() {
		ep := strings.TrimRight(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), "/")
		exp = &exporter{endpoint: ep, queue: make(chan finished, 4096), client: &http.Client{Timeout: 3 * time.Second}}
		if ep != "" {
			go exp.loop()
		}
	})
	return exp
}

func (e *exporter) enqueue(s *Span, end time.Time) {
	if e.endpoint == "" {
		return
	}
	select {
	case e.queue <- finished{s, end}:
	default:
		spansDropped.Inc(s.svc)
	}
}

func (e *exporter) loop() {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	batch := make([]finished, 0, 256)
	for {
		select {
		case f := <-e.queue:
			batch = append(batch, f)
			if len(batch) < 256 {
				continue
			}
		case <-tick.C:
		}
		if len(batch) > 0 {
			e.send(batch)
			batch = batch[:0]
		}
	}
}

// FlushTraces gives the exporter up to d to send what is queued (tests, shutdown).
func FlushTraces(d time.Duration) {
	e := defaultExporter()
	if e.endpoint == "" {
		return
	}
	deadline := time.Now().Add(d)
	for len(e.queue) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(min(1100*time.Millisecond, time.Until(deadline)))
}

func (e *exporter) send(batch []finished) {
	bySvc := map[string][]map[string]any{}
	for _, f := range batch {
		bySvc[f.s.svc] = append(bySvc[f.s.svc], otlpSpan(f.s, f.end))
	}
	var rs []map[string]any
	for svc, spans := range bySvc {
		rs = append(rs, map[string]any{
			"resource": map[string]any{"attributes": otlpAttrs(map[string]any{
				"service.name": svc, "service.instance.id": instanceName(),
			})},
			"scopeSpans": []map[string]any{{"scope": map[string]any{"name": "argus/pkg/obs"}, "spans": spans}},
		})
	}
	body, err := json.Marshal(map[string]any{"resourceSpans": rs})
	if err != nil {
		return
	}
	resp, err := e.client.Post(e.endpoint+"/v1/traces", "application/json", bytes.NewReader(body))
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			err = fmt.Errorf("collector answered %d", resp.StatusCode)
		}
	}
	for svc, spans := range bySvc {
		if err != nil {
			spansDropped.Add(float64(len(spans)), svc)
		} else {
			spansExported.Add(float64(len(spans)), svc)
		}
	}
}

func otlpSpan(s *Span, end time.Time) map[string]any {
	s.mu.Lock()
	attrs := make(map[string]any, len(s.attrs))
	for k, v := range s.attrs {
		attrs[k] = v
	}
	errMsg := s.errMsg
	s.mu.Unlock()
	out := map[string]any{
		"traceId":           hex.EncodeToString(s.sc.TraceID[:]),
		"spanId":            hex.EncodeToString(s.sc.SpanID[:]),
		"name":              s.name,
		"kind":              int(s.kind),
		"startTimeUnixNano": strconv.FormatInt(s.start.UnixNano(), 10),
		"endTimeUnixNano":   strconv.FormatInt(end.UnixNano(), 10),
		"attributes":        otlpAttrs(attrs),
	}
	if s.parent != [8]byte{} {
		out["parentSpanId"] = hex.EncodeToString(s.parent[:])
	}
	if errMsg != "" {
		out["status"] = map[string]any{"code": 2, "message": errMsg}
	}
	return out
}

func otlpAttrs(m map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(m))
	for k, v := range m {
		var val map[string]any
		switch x := v.(type) {
		case string:
			val = map[string]any{"stringValue": x}
		case bool:
			val = map[string]any{"boolValue": x}
		case int:
			val = map[string]any{"intValue": strconv.Itoa(x)}
		case int64:
			val = map[string]any{"intValue": strconv.FormatInt(x, 10)}
		case float64:
			if math.IsNaN(x) || math.IsInf(x, 0) {
				val = map[string]any{"stringValue": fmtFloat(x)}
			} else {
				val = map[string]any{"doubleValue": x}
			}
		default:
			val = map[string]any{"stringValue": fmt.Sprint(x)}
		}
		out = append(out, map[string]any{"key": k, "value": val})
	}
	return out
}

func instanceName() string {
	if v := os.Getenv("POD_NAME"); v != "" {
		return v
	}
	h, _ := os.Hostname()
	return h
}

// ---------------------------------------------------------- service name ---

var svcName atomic.Value // string

// SetServiceName names this process in metrics and traces (NewLogger calls it).
func SetServiceName(name string) { svcName.Store(name) }

func serviceName() string {
	if v, ok := svcName.Load().(string); ok && v != "" {
		return v
	}
	return "unknown"
}
