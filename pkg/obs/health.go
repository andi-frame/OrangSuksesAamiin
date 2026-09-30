package obs

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Check is a readiness probe (e.g. database ping). A nil error means healthy.
type Check func(ctx context.Context) error

// LivenessHandler answers 200 as long as the process serves HTTP. Extra fields
// (e.g. the mock outage state) may be supplied through extra.
func LivenessHandler(service string, extra func() map[string]any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body := map[string]any{"status": "ok", "service": service, "time": time.Now().UTC().Format(time.RFC3339Nano)}
		if extra != nil {
			for k, v := range extra() {
				body[k] = v
			}
		}
		WriteJSON(w, http.StatusOK, body)
	}
}

// ReadinessHandler runs the checks and answers 503 when any fails.
func ReadinessHandler(service string, checks map[string]Check) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		status, code := "ready", http.StatusOK
		res := map[string]string{}
		for name, c := range checks {
			if err := c(ctx); err != nil {
				res[name] = err.Error()
				status, code = "not_ready", http.StatusServiceUnavailable
			} else {
				res[name] = "ok"
			}
		}
		WriteJSON(w, code, map[string]any{"status": status, "service": service, "checks": res})
	}
}

// WriteJSON writes v as JSON with the given status.
func WriteJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes the uniform error envelope {"error":{"code","message"}}.
func WriteError(w http.ResponseWriter, code int, errCode, msg string) {
	WriteJSON(w, code, map[string]any{"error": map[string]string{"code": errCode, "message": msg}})
}
