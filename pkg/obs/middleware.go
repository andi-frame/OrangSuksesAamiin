package obs

import (
	"log/slog"
	"net/http"
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
func Middleware(log *slog.Logger, quiet ...string) func(http.Handler) http.Handler {
	quietSet := map[string]bool{}
	for _, p := range quiet {
		quietSet[p] = true
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get(HeaderCorrelationID)
			if id == "" {
				id = NewCorrelationID()
			}
			ctx := WithCorrelationID(r.Context(), id)
			w.Header().Set(HeaderCorrelationID, id)
			sw := &statusWriter{ResponseWriter: w}
			start := time.Now()
			next.ServeHTTP(sw, r.WithContext(ctx))
			if sw.status == 0 {
				sw.status = http.StatusOK
			}
			lvl := slog.LevelInfo
			if quietSet[r.URL.Path] {
				lvl = slog.LevelDebug
			}
			log.Log(ctx, lvl, "request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.status,
				"duration_ms", float64(time.Since(start).Microseconds())/1000,
				"bytes", sw.bytes,
				"remote", r.RemoteAddr,
			)
		})
	}
}
