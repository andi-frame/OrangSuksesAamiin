package bmkg

import (
	"sync"
	"time"
)

// SeismicEvent is the BMKG entity from the M1 contract.
type SeismicEvent struct {
	EventID          string    `json:"event_id"`
	Magnitude        float64   `json:"magnitude"`
	DepthKm          float64   `json:"depth_km"`
	EpicenterLat     float64   `json:"epicenter_lat"`
	EpicenterLon     float64   `json:"epicenter_lon"`
	RegionName       string    `json:"region_name"`
	OccurredAt       time.Time `json:"occurred_at"`
	PotentialTsunami bool      `json:"potential_tsunami"`
}

// TsunamiWarning is the BMKG entity from the M1 contract. IssuedAt is an
// allowed addition (contract: "Penambahan ... diperbolehkan") used as the
// `since` cursor of GET /tsunami-warnings.
type TsunamiWarning struct {
	WarningID        string    `json:"warning_id"`
	RelatedEventID   string    `json:"related_event_id"`
	ThreatLevel      string    `json:"threat_level"` // Waspada | Siaga | Awas
	AffectedZones    []string  `json:"affected_zones"`
	EstimatedArrival time.Time `json:"estimated_arrival"`
	IssuedAt         time.Time `json:"issued_at"`
}

// Store is the in-memory data set owned by the mock.
type Store struct {
	mu       sync.RWMutex
	events   []SeismicEvent // ascending by OccurredAt
	warnings []TsunamiWarning
}

func NewStore() *Store { return &Store{} }

func (s *Store) AddEvent(e SeismicEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
}

func (s *Store) AddWarning(w TsunamiWarning) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.warnings = append(s.warnings, w)
}

// EventsSince returns events with OccurredAt strictly after since.
func (s *Store) EventsSince(since time.Time) []SeismicEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]SeismicEvent, 0, len(s.events))
	for _, e := range s.events {
		if e.OccurredAt.After(since) {
			out = append(out, e)
		}
	}
	return out
}

// WarningsSince returns warnings with IssuedAt strictly after since.
func (s *Store) WarningsSince(since time.Time) []TsunamiWarning {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]TsunamiWarning, 0, len(s.warnings))
	for _, w := range s.warnings {
		if w.IssuedAt.After(since) {
			out = append(out, w)
		}
	}
	return out
}

// Counts returns (events, warnings) currently stored.
func (s *Store) Counts() (int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events), len(s.warnings)
}
