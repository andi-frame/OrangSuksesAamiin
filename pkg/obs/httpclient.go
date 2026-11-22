package obs

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// loggingTransport propagates the correlation ID and records the latency of
// every outbound call (an "outbound_call" log line).
type loggingTransport struct {
	base   http.RoundTripper
	log    *slog.Logger
	target string
}

func (t loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	ctx, span := StartSpan(ctx, req.Method+" "+t.target, SpanClient,
		"peer.service", t.target, "http.method", req.Method, "http.url", req.URL.Scheme+"://"+req.URL.Host+req.URL.Path)
	defer span.End()
	req = req.Clone(ctx)
	if id := CorrelationID(ctx); id != "" && req.Header.Get(HeaderCorrelationID) == "" {
		req.Header.Set(HeaderCorrelationID, id)
	}
	req.Header.Set(HeaderTraceparent, span.SpanContext().Traceparent())
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	took := time.Since(start)
	ms := float64(took.Microseconds()) / 1000
	svc := serviceName()
	clientDuration.Observe(took.Seconds(), svc, t.target)
	attrs := []any{
		"target", t.target,
		"method", req.Method,
		"url", req.URL.Scheme + "://" + req.URL.Host + req.URL.Path,
		"latency_ms", ms,
	}
	if err != nil {
		clientRequests.Inc(svc, t.target, "error")
		span.RecordError(err)
		t.log.WarnContext(ctx, "outbound_call", append(attrs, "error", err.Error())...)
		return nil, err
	}
	clientRequests.Inc(svc, t.target, strconv.Itoa(resp.StatusCode))
	span.SetAttr("http.status_code", resp.StatusCode)
	lvl := slog.LevelInfo
	if resp.StatusCode >= 500 {
		lvl = slog.LevelWarn
		span.RecordError(fmt.Errorf("HTTP %d", resp.StatusCode))
	}
	t.log.Log(ctx, lvl, "outbound_call", append(attrs, "status", resp.StatusCode)...)
	return resp, nil
}

// NewHTTPClient returns an *http.Client whose every call forwards the
// correlation ID of the request context and logs its latency. target is a
// short name for the callee (e.g. "bmkg").
func NewHTTPClient(log *slog.Logger, target string, timeout time.Duration) *http.Client {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.MaxIdleConnsPerHost = 16
	return &http.Client{
		Timeout:   timeout,
		Transport: loggingTransport{base: base, log: log, target: target},
	}
}
