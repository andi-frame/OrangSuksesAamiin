package pvmbg

import (
	"encoding/json"
	"sync"
	"time"
)

// Report is the PVMBG VolcanicReport as stored. Confidence is only set on
// reports created while the schema-v2 switch was on. Extra holds operator
// injected fields (POST /admin/schema-version extra_fields, or a per-report
// override); a nil value in Extra removes that field from the wire format.
type Report struct {
	ReportID         string
	VolcanoID        string
	AlertLevel       string
	EruptionCount24h int
	AshColumnHeightM float64
	ReportedAt       time.Time
	Confidence       *float64
	Extra            map[string]any
}

// Wire is one report rendered for the response.
type Wire struct {
	ReportID   string
	ReportedAt time.Time
	Fields     map[string]any
}

func (w Wire) MarshalJSON() ([]byte, error) { return json.Marshal(w.Fields) }

// render builds the wire form. confidence_level is emitted only when the report
// carries one AND the schema-v2 switch is currently on.
func (r Report) render(schemaV2 bool) Wire {
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
	for k, v := range r.Extra {
		if v == nil {
			delete(f, k)
		} else {
			f[k] = v
		}
	}
	return Wire{ReportID: r.ReportID, ReportedAt: r.ReportedAt, Fields: f}
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
