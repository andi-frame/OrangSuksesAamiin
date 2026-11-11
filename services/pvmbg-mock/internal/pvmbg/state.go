package pvmbg

import (
	"slices"
	"sync"
	"time"
)

// State is the operator-controlled runtime state of the mock. All of it can
// change while the process runs (no restart), which is what the demo needs.
type State struct {
	mu           sync.RWMutex
	confidence   bool           // schema v2: confidence_level appears on new reports
	extra        map[string]any // operator-injected extra fields for new reports
	changes      []string       // incompatible changes applied to new reports (M2)
	truncateNext int            // the next n responses are cut off mid-element (M2)
	outage       bool
	outageMode   string // "error" (503) | "hang" (no answer)
	delayMin     time.Duration
	delayMax     time.Duration
}

// Snapshot is a consistent copy of the state, safe to serialise.
type Snapshot struct {
	ConfidenceLevel     bool           `json:"confidence_level"`
	ExtraFields         map[string]any `json:"extra_fields"`
	IncompatibleChanges []string       `json:"incompatible_changes"`
	TruncateNext        int            `json:"truncate_next_responses"`
	SchemaVersion       int            `json:"schema_version"`
	Outage              bool           `json:"outage"`
	OutageMode          string         `json:"outage_mode"`
	DelayMinMs          int64          `json:"delay_min_ms"`
	DelayMaxMs          int64          `json:"delay_max_ms"`
}

func NewState(cfg Config) *State {
	return &State{delayMin: cfg.DelayMin, delayMax: cfg.DelayMax, outageMode: "error"}
}

func (s *State) Snapshot() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v := 1
	if s.confidence {
		v = 2
	}
	if len(s.changes) > 0 {
		v = 3
	}
	return Snapshot{
		ConfidenceLevel: s.confidence, SchemaVersion: v, ExtraFields: s.extraCopyLocked(),
		IncompatibleChanges: append([]string{}, s.changes...), TruncateNext: s.truncateNext,
		Outage: s.outage, OutageMode: s.outageMode,
		DelayMinMs: s.delayMin.Milliseconds(), DelayMaxMs: s.delayMax.Milliseconds(),
	}
}

func (s *State) Confidence() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.confidence
}

func (s *State) SetConfidence(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.confidence = on
}

// Changes returns the incompatible changes applied to new reports.
func (s *State) Changes() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.changes)
}

// SetChanges replaces them (empty = the compatible schema again).
func (s *State) SetChanges(c []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes = slices.Clone(c)
}

// AddTruncate makes the next n responses arrive cut off.
func (s *State) AddTruncate(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truncateNext += n
}

// TakeTruncate reports whether this response must be cut, consuming one.
func (s *State) TakeTruncate() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.truncateNext == 0 {
		return false
	}
	s.truncateNext--
	return true
}

func (s *State) Outage() (bool, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.outage, s.outageMode
}

func (s *State) SetOutage(on bool, mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outage = on
	if mode == "error" || mode == "hang" {
		s.outageMode = mode
	}
}

func (s *State) Delay() (time.Duration, time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.delayMin, s.delayMax
}

func (s *State) SetDelay(lo, hi time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delayMin, s.delayMax = lo, hi
}

// Extra returns a copy of the operator-injected extra fields.
func (s *State) Extra() map[string]any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]any, len(s.extra))
	for k, v := range s.extra {
		out[k] = v
	}
	return out
}

// SetExtra replaces the extra fields added to every report created from now on
// (an empty map clears them).
func (s *State) SetExtra(m map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.extra = m
}

func (s *State) extraCopyLocked() map[string]any {
	out := make(map[string]any, len(s.extra))
	for k, v := range s.extra {
		out[k] = v
	}
	return out
}
