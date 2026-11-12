package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/aggregator/internal/ports"
)

// auditItem is the Lampiran A shape: metadata only, never the payload.
type auditItem struct {
	MessageID  string    `json:"message_id"`
	Source     string    `json:"source"`
	ReceivedAt time.Time `json:"received_at"`
	Reason     string    `json:"reason"`
	Retryable  bool      `json:"retryable"`
	Attempts   int       `json:"attempts"`
	Status     string    `json:"status"`
}

// opsItem adds what Internal Ops needs to understand and fix a failure,
// including the exact bytes received.
type opsItem struct {
	auditItem
	Seq           int64      `json:"seq"`
	Kind          string     `json:"kind"`
	RecordKey     string     `json:"record_key"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
	ResolvedAt    *time.Time `json:"resolved_at"`
	CorrelationID string     `json:"correlation_id"`
	RawSize       int64      `json:"raw_size"`
	RawStored     int        `json:"raw_stored_bytes"`
	RawText       *string    `json:"raw_text"`   // the bytes as text when they are valid UTF-8
	RawBase64     string     `json:"raw_base64"` // always: the exact bytes
}

func toAudit(m ports.Unprocessed) auditItem {
	return auditItem{MessageID: m.MessageID, Source: m.Source, ReceivedAt: m.ReceivedAt, Reason: m.Reason,
		Retryable: m.Retryable, Attempts: m.Attempts, Status: m.Status}
}

func toOps(m ports.Unprocessed, withRaw bool) opsItem {
	it := opsItem{auditItem: toAudit(m), Seq: m.Seq, Kind: m.Kind, RecordKey: m.RecordKey, ResolvedAt: m.ResolvedAt,
		CorrelationID: m.CorrelationID, RawSize: m.RawSize, RawStored: len(m.Raw)}
	if m.Status == ports.StatusRetrying {
		t := m.NextAttemptAt
		it.NextAttemptAt = &t
	}
	if withRaw {
		it.RawBase64 = base64.StdEncoding.EncodeToString(m.Raw)
		if utf8.Valid(m.Raw) {
			s := string(m.Raw)
			it.RawText = &s
		}
	}
	return it
}

// filter parses source / limit / cursor (and status when allowStatus).
func filter(w http.ResponseWriter, r *http.Request, allowStatus bool) (ports.UnprocessedFilter, bool) {
	q := r.URL.Query()
	f := ports.UnprocessedFilter{Source: strings.ToUpper(q.Get("source")), Cursor: q.Get("cursor")}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "limit must be an integer between 1 and 1000")
			return f, false
		}
		f.Limit = n
	}
	if f.Cursor != "" {
		if _, err := ports.DecodeSeqCursor(f.Cursor); err != nil {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "invalid cursor")
			return f, false
		}
	}
	if v := q.Get("status"); allowStatus && v != "" {
		switch v {
		case ports.StatusRetrying, ports.StatusParked, ports.StatusResolved:
			f.Statuses = []string{v}
		case "all":
			f.Statuses = []string{ports.StatusRetrying, ports.StatusParked, ports.StatusResolved}
		default:
			obs.WriteError(w, http.StatusBadRequest, "bad_request", "status must be retrying, parked, resolved or all")
			return f, false
		}
	}
	return f, true
}

func nextCursor(c string) any {
	if c == "" {
		return nil
	}
	return c
}

// GET /internal/audit/unprocessed (Lampiran A): open messages, oldest first.
func (s *Server) auditUnprocessed(w http.ResponseWriter, r *http.Request) {
	f, ok := filter(w, r, false)
	if !ok {
		return
	}
	page, err := s.Repo.ListUnprocessed(r.Context(), f)
	if err != nil {
		s.Log.ErrorContext(r.Context(), "audit_unprocessed_failed", "error", err.Error())
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable", "dead-letter store unavailable")
		return
	}
	items := make([]auditItem, 0, len(page.Items))
	for _, m := range page.Items {
		items = append(items, toAudit(m))
	}
	obs.WriteJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": nextCursor(page.NextCursor)})
}

// GET /internal/ops/unprocessed?source=&status=&limit=&cursor=
func (s *Server) opsList(w http.ResponseWriter, r *http.Request) {
	f, ok := filter(w, r, true)
	if !ok {
		return
	}
	page, err := s.Repo.ListUnprocessed(r.Context(), f)
	if err != nil {
		s.Log.ErrorContext(r.Context(), "ops_unprocessed_failed", "error", err.Error())
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable", "dead-letter store unavailable")
		return
	}
	items := make([]opsItem, 0, len(page.Items))
	for _, m := range page.Items {
		items = append(items, toOps(m, r.URL.Query().Get("raw") != "false"))
	}
	body := map[string]any{"items": items, "next_cursor": nextCursor(page.NextCursor)}
	if c, err := s.Repo.CountUnprocessed(r.Context()); err == nil {
		body["open"] = c
	}
	obs.WriteJSON(w, http.StatusOK, body)
}

// GET /internal/ops/unprocessed/{id}
func (s *Server) opsGet(w http.ResponseWriter, r *http.Request) {
	m, err := s.Repo.GetUnprocessed(r.Context(), r.PathValue("id"))
	switch {
	case errors.Is(err, ports.ErrNotFound):
		obs.WriteError(w, http.StatusNotFound, "not_found", "message not found")
	case err != nil:
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable", "dead-letter store unavailable")
	default:
		obs.WriteJSON(w, http.StatusOK, toOps(*m, true))
	}
}

// POST /internal/ops/unprocessed/reprocess {"source":"PVMBG","message_ids":[...]}
// Parked messages go back to "retrying", due now. The leader's worker processes
// them in arrival order (per record), through the current code and schema rules.
func (s *Server) opsReprocess(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Source     string   `json:"source"`
		MessageIDs []string `json:"message_ids"`
	}
	if r.ContentLength != 0 {
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			obs.WriteError(w, http.StatusBadRequest, "bad_request", `body must be {"source":"PVMBG","message_ids":["msg-..."]} (both optional)`)
			return
		}
	}
	n, err := s.Repo.RequeueUnprocessed(r.Context(), strings.ToUpper(body.Source), body.MessageIDs, s.now())
	if err != nil {
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable", "dead-letter store unavailable")
		return
	}
	s.Log.InfoContext(r.Context(), "unprocessed_requeued", "source", body.Source, "ids", len(body.MessageIDs), "requeued", n)
	obs.WriteJSON(w, http.StatusAccepted, map[string]any{
		"requeued": n,
		"note":     "the leader retries them in arrival order within a few seconds; see ?status=resolved and ?status=parked",
	})
}

// GET /internal/ops/schema-rules: what the running instance applies.
func (s *Server) opsRules(w http.ResponseWriter, r *http.Request) {
	rules := s.Rules
	if rules == nil {
		rules = []string{}
	}
	obs.WriteJSON(w, http.StatusOK, map[string]any{"rules": rules})
}
