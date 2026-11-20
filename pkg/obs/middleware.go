package obs

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

type statusWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}

// Flush keeps http.Flusher working through the wrapper.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// Middleware reads (or creates) the correlation ID, echoes it in the
// response, and logs one structured "request" line per inbound request.
// Paths listed in quiet (e.g. /health) are logged at debug level to avoid noise.
//
// M2 observability, for every service that uses it:
//   - GET /metrics serves the Prometheus registry (request count, latency
//     histogram and in-flight gauge per route, plus whatever the service adds);
//   - every non-quiet request is a server span that continues an incoming
//     "traceparent" (or starts the trace of this correlation ID), and the
//     response carries the traceparent so a caller can look the trace up.
func Middleware(log *slog.Logger, quiet ...string) func(http.Handler) http.Handler {
	quietSet := map[string]bool{}
	for _, p := range quiet {
		quietSet[p] = true
	}
	metrics := DefaultRegistry.MetricsHandler()
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/metrics" && r.Method == http.MethodGet {
				metrics.ServeHTTP(w, r)
				return
			}
			id := r.Header.Get(HeaderCorrelationID)
			if id == "" {
				id = NewCorrelationID()
			}
			ctx := WithCorrelationID(r.Context(), id)
			w.Header().Set(HeaderCorrelationID, id)
			quietPath := quietSet[r.URL.Path]

			var span *Span
			if !quietPath {
				if sc, ok := ParseTraceparent(r.Header.Get(HeaderTraceparent)); ok {
					ctx = ContextWithSpanContext(ctx, sc)
				}
				ctx, span = StartSpan(ctx, r.Method+" "+r.URL.Path, SpanServer,
					"http.method", r.Method, "http.target", r.URL.Path, "http.caller", r.Header.Get(HeaderCaller))
				w.Header().Set(HeaderTraceparent, span.SpanContext().Traceparent())
			}
			svc := serviceName()
			httpInFlight.Add(1, svc)
			sw := &statusWriter{ResponseWriter: w}
			start := time.Now()
			req := r.WithContext(ctx)
			next.ServeHTTP(sw, req)
			httpInFlight.Add(-1, svc)
			if sw.status == 0 {
				sw.status = http.StatusOK
			}
			took := time.Since(start)
			route := routeOf(req)
			if !quietPath {
				httpRequests.Inc(svc, r.Method, route, strconv.Itoa(sw.status))
				httpDuration.Observe(took.Seconds(), svc, r.Method, route)
			}
			if span != nil {
				span.name = r.Method + " " + route
				span.SetAttr("http.route", route)
				span.SetAttr("http.status_code", sw.status)
				if sw.status >= 500 {
					span.RecordError(fmt.Errorf("HTTP %d", sw.status))
				}
				span.End()
			}
			lvl := slog.LevelInfo
			if quietPath {
				lvl = slog.LevelDebug
			}
			log.Log(ctx, lvl, "request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.status,
				"duration_ms", float64(took.Microseconds())/1000,
				"bytes", sw.bytes,
				"remote", r.RemoteAddr,
				"caller", r.Header.Get(HeaderCaller),
			)
		})
	}
}
