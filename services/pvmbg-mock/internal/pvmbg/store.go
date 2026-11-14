package pvmbg

import (
	"bytes"
	"encoding/json"
	"sync"
	"time"
)

// Incompatible schema changes (M2 Problem 3). Each applies to the reports
// created while it is switched on, like a new major version of the PVMBG API:
//
//	rename         alert_level            -> level
//	type_change    reported_at (ISO text) -> reported_at (Unix seconds, number)
//	drop_required  volcano_id             -> gone; the code moves to location.volcano_code
const (
	ChangeRename       = "rename"
	ChangeTypeChange   = "type_change"
	ChangeDropRequired = "drop_required"
)

// AllChanges is schema "version 3": every incompatible change at once.
var AllChanges = []string{ChangeRename, ChangeTypeChange, ChangeDropRequired}

// Report is the PVMBG VolcanicReport as stored. Confidence is only set on
// reports created while the schema-v2 switch was on. Extra holds operator
// injected fields (POST /admin/schema-version extra_fields, or a per-report
// override); a nil value in Extra removes that field from the wire format.
// Changes lists the incompatible changes active when the report was created.
// Raw, when set, is sent verbatim instead of the rendered fields (a malformed
// message: POST /admin/malformed).
type Report struct {
	ReportID         string
	VolcanoID        string
	AlertLevel       string
	EruptionCount24h int
	AshColumnHeightM float64
	ReportedAt       time.Time
	Confidence       *float64
	Extra            map[string]any
	Changes          []string
	Raw              []byte
}

// Wire is one report rendered for the response.
type Wire struct {
	ReportID   string
	ReportedAt time.Time
	Fields     map[string]any
	Raw        []byte // verbatim bytes (malformed messages); Fields is nil then
}

func (w Wire) MarshalJSON() ([]byte, error) { return json.Marshal(w.Fields) }

// render builds the wire form. confidence_level is emitted only when the report
// carries one AND the schema-v2 switch is currently on.
func (r Report) render(schemaV2 bool) Wire {
	if r.Raw != nil {
		return Wire{ReportID: r.ReportID, ReportedAt: r.ReportedAt, Raw: r.Raw}
	}
	f := map[string]any{
		"report_id":           r.ReportID,
		"volcano_id":          r.VolcanoID,
		"alert_level":         r.AlertLevel,
		"eruption_count_24h":  r.EruptionCount24h,
		"ash_column_height_m": r.AshColumnHeightM,
		"reported_at":         r.ReportedAt,
	}
	if schemaV2 && r.Confidence != nil {
		f["confidence_level"] = *r.Confidence
	}
	for _, c := range r.Changes {
		switch c {
		case ChangeRename:
			f["level"] = f["alert_level"]
			delete(f, "alert_level")
		case ChangeTypeChange:
			f["reported_at"] = r.ReportedAt.Unix()
		case ChangeDropRequired:
			f["location"] = map[string]any{"volcano_code": r.VolcanoID}
			delete(f, "volcano_id")
		}
	}
	for k, v := range r.Extra {
		if v == nil {
			delete(f, k)
		} else {
			f[k] = v
		}
	}
	return Wire{ReportID: r.ReportID, ReportedAt: r.ReportedAt, Fields: f}
}

// EncodeArray writes the wires as one JSON array. Malformed reports are
// written byte for byte, so the body may on purpose not be valid JSON. When
// truncate is set, the body is cut two thirds into the last element, like a
// connection that dropped mid-transfer.
func EncodeArray(ws []Wire, truncate bool) ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	lastStart := -1
	for i, w := range ws {
		if i > 0 {
			buf.WriteByte(',')
		}
		lastStart = buf.Len()
		if w.Raw != nil {
			buf.Write(w.Raw)
			continue
		}
		b, err := json.Marshal(w.Fields)
		if err != nil {
			return nil, err
		}
		buf.Write(b)
	}
	if truncate && lastStart >= 0 {
		cut := lastStart + (buf.Len()-lastStart)*2/3
		return buf.Bytes()[:cut], nil
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

// Store is the in-memory data set owned by the mock.
type Store struct {
	mu      sync.RWMutex
	reports []Report // ascending by ReportedAt
}

func NewStore() *Store { return &Store{} }

func (s *Store) Add(r Report) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports = append(s.reports, r)
}

// Since returns reports strictly after since, rendered for the current schema.
func (s *Store) Since(since time.Time, schemaV2 bool) []Wire {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Wire, 0, len(s.reports))
	for _, r := range s.reports {
		if r.ReportedAt.After(since) {
			out = append(out, r.render(schemaV2))
		}
	}
	return out
}

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.reports)
}
