package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/andi-frame/orangsuksesaamiin/pkg/obs"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/auth"
	"github.com/andi-frame/orangsuksesaamiin/services/client-api/internal/reports"
)

var (
	fieldReports = obs.NewCounter("client_api_field_reports_total",
		"POST /v1/field-reports outcomes: accepted (202, durable in the broker), invalid (400), forbidden, unavailable (503).", "outcome")
	fieldReportAck = obs.NewHistogram("client_api_field_report_ack_seconds",
		"Time to make a field report durable (broker acknowledgement) before answering 202.",
		[]float64{0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5})
)

// postFieldReport: POST /v1/field-reports (Tim Lapangan only).
func (s *Server) postFieldReport(w http.ResponseWriter, r *http.Request) {
	if auth.Role(r.Context()) != "lapangan" {
		fieldReports.Inc("forbidden")
		obs.WriteError(w, http.StatusForbidden, "forbidden", "only Tim Lapangan clients may submit field reports")
		return
	}
	if s.Reports == nil {
		fieldReports.Inc("unavailable")
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable", "field reports are not configured")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, reports.MaxBodyBytes+1))
	if err != nil || len(body) > reports.MaxBodyBytes {
		fieldReports.Inc("invalid")
		obs.WriteError(w, http.StatusBadRequest, "bad_request", "body missing or larger than 64 KiB")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	report, err := reports.Validate(key, body)
	var inv *reports.ErrInvalid
	if errors.As(err, &inv) {
		fieldReports.Inc("invalid")
		obs.WriteError(w, http.StatusBadRequest, "bad_request", inv.Msg)
		return
	}
	client := auth.Subject(r.Context())
	env := reports.Envelope{
		ReportID: reports.ReportID(client, key), IdempotencyKey: key, ClientID: client,
		ReceivedAt: time.Now().UTC(), Report: report,
	}
	val, _ := json.Marshal(env)

	ctx, span := obs.StartSpan(r.Context(), "produce "+reports.TopicIncoming, obs.SpanProducer, "report_id", env.ReportID)
	headers := map[string]string{"report_id": env.ReportID}
	obs.InjectHeaders(ctx, headers)
	timeout := s.ReportAckTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	start := time.Now()
	err = s.Reports.Publish(pctx, env.ReportID, val, headers)
	cancel()
	span.RecordError(err)
	span.End()
	fieldReportAck.ObserveSince(start)
	if err != nil {
		fieldReports.Inc("unavailable")
		s.Log.WarnContext(r.Context(), "field_report_not_accepted", "report_id", env.ReportID, "error", err.Error())
		w.Header().Set("Retry-After", "2")
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable",
			"report not accepted; retry with the same Idempotency-Key")
		return
	}
	fieldReports.Inc("accepted")
	s.Log.InfoContext(r.Context(), "field_report_accepted", "report_id", env.ReportID, "client_id", client,
		"idempotency_key", key, "ack_ms", float64(time.Since(start).Microseconds())/1000)
	w.Header().Set("Location", "/v1/field-reports/"+env.ReportID)
	obs.WriteJSON(w, http.StatusAccepted, map[string]any{
		"report_id": env.ReportID, "status": "accepted", "idempotency_key": key, "received_at": env.ReceivedAt,
	})
}

// getFieldReport: GET /v1/field-reports/{id} (Lapangan: own reports; Pusat: all).
func (s *Server) getFieldReport(w http.ResponseWriter, r *http.Request) {
	role := auth.Role(r.Context())
	if role != "lapangan" && role != "pusat" {
		obs.WriteError(w, http.StatusForbidden, "forbidden", "endpoint not allowed for this client role")
		return
	}
	if s.ReportsURL == "" {
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable", "field reports are not configured")
		return
	}
	req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, s.ReportsURL+"/reports/"+url.PathEscape(r.PathValue("id")), nil)
	resp, err := s.reportsClient.Do(req)
	if err != nil {
		obs.WriteError(w, http.StatusServiceUnavailable, "unavailable", "report status temporarily unavailable")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		obs.WriteError(w, http.StatusNotFound, "not_found", "no processed report with this id (yet)")
		return
	}
	var rec struct {
		ClientID string `json:"client_id"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK || json.Unmarshal(b, &rec) != nil {
		obs.WriteError(w, http.StatusBadGateway, "bad_gateway", "report status unavailable")
		return
	}
	if role == "lapangan" && rec.ClientID != auth.Subject(r.Context()) {
		obs.WriteError(w, http.StatusNotFound, "not_found", "no processed report with this id (yet)")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
