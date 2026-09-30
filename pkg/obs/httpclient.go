package obs

import (
	"log/slog"
	"net/http"
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
	if id := CorrelationID(ctx); id != "" && req.Header.Get(HeaderCorrelationID) == "" {
		req = req.Clone(ctx)
		req.Header.Set(HeaderCorrelationID, id)
	}
	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	ms := float64(time.Since(start).Microseconds()) / 1000
	attrs := []any{
		"target", t.target,
		"method", req.Method,
		"url", req.URL.Scheme + "://" + req.URL.Host + req.URL.Path,
		"latency_ms", ms,
	}
	if err != nil {
		t.log.WarnContext(ctx, "outbound_call", append(attrs, "error", err.Error())...)
		return nil, err
	}
	lvl := slog.LevelInfo
	if resp.StatusCode >= 500 {
		lvl = slog.LevelWarn
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
