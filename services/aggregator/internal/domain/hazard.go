// Package domain holds the canonical BNPB entities. It has no I/O.
package domain

import (
	"encoding/json"
	"reflect"
	"strconv"
	"time"
)

type Source string

const (
	SourceBMKG  Source = "BMKG"
	SourcePVMBG Source = "PVMBG"
)

type HazardType string

const (
	HazardSeismic  HazardType = "SEISMIC"
	HazardVolcanic HazardType = "VOLCANIC"
)

type Severity string

const (
	SeverityNormal  Severity = "NORMAL"
	SeverityWaspada Severity = "WASPADA"
	SeveritySiaga   Severity = "SIAGA"
	SeverityAwas    Severity = "AWAS"
)

// Rank orders severities (higher = more severe).
func (s Severity) Rank() int {
	switch s {
	case SeverityNormal:
		return 0
	case SeverityWaspada:
		return 1
	case SeveritySiaga:
		return 2
	case SeverityAwas:
		return 3
	}
	return -1
}

// HazardEvent is the canonical representation (M1 spec "Format Kanonik").
//
// Field classification (enforced by the Client-Facing API):
//
//	Ringkasan: hazard_id, source, hazard_type, severity, area_name, occurred_at,
//	           ingested_at  (+ our additions: version, updated_at)
//	Mentah:    source_ref_id, latitude, longitude, attributes
//
// Version and UpdatedAt are additions: Version increases whenever the content of
// the same hazard changes (e.g. a late tsunami warning raises the severity), which
// lets consumers de-duplicate on "hazard_id:version".
type HazardEvent struct {
	HazardID    string         `json:"hazard_id"`
	Source      Source         `json:"source"`
	SourceRefID string         `json:"source_ref_id"`
	HazardType  HazardType     `json:"hazard_type"`
	Severity    Severity       `json:"severity"`
	AreaName    string         `json:"area_name"`
	Latitude    *float64       `json:"latitude"`
	Longitude   *float64       `json:"longitude"`
	OccurredAt  time.Time      `json:"occurred_at"`
	IngestedAt  time.Time      `json:"ingested_at"`
	Attributes  map[string]any `json:"attributes"`
	Version     int            `json:"version"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// SummaryFields lists the canonical fields classified as Ringkasan (summary).
var SummaryFields = []string{
	"hazard_id", "source", "hazard_type", "severity", "area_name",
	"occurred_at", "ingested_at", "version", "updated_at",
}

// HazardID builds the deterministic canonical identifier, so re-ingesting the
// same source record is idempotent: "bmkg-<event_id>" / "pvmbg-<report_id>".
func HazardID(src Source, refID string) string {
	switch src {
	case SourceBMKG:
		return "bmkg-" + refID
	default:
		return "pvmbg-" + refID
	}
}

// EventID is the idempotency key of one published version.
func EventID(hazardID string, version int) string {
	return hazardID + ":" + strconv.Itoa(version)
}

func eqFloatPtr(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// SameContent reports whether two versions of a hazard are equal in every field
// that matters for consumers. IngestedAt, UpdatedAt and Version are excluded,
// which makes re-polling an unchanged source record a no-op.
func SameContent(a, b HazardEvent) bool {
	return a.HazardID == b.HazardID &&
		a.Source == b.Source &&
		a.SourceRefID == b.SourceRefID &&
		a.HazardType == b.HazardType &&
		a.Severity == b.Severity &&
		a.AreaName == b.AreaName &&
		eqFloatPtr(a.Latitude, b.Latitude) &&
		eqFloatPtr(a.Longitude, b.Longitude) &&
		a.OccurredAt.Equal(b.OccurredAt) &&
		reflect.DeepEqual(nz(a.Attributes), nz(b.Attributes))
}

func nz(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// Envelope is the message published to the broker (subject/topic hazard.events).
type Envelope struct {
	EventID    string      `json:"event_id"` // "<hazard_id>:<version>", the idempotency key
	Kind       string      `json:"kind"`     // "created" | "updated"
	EmittedAt  time.Time   `json:"emitted_at"`
	Hazard     HazardEvent `json:"hazard"`
	SchemaName string      `json:"schema"` // "hazard-event/v1"
}

const (
	KindCreated = "created"
	KindUpdated = "updated"
	SchemaName  = "hazard-event/v1"
)

// Clone returns a deep copy (attributes may contain nested maps/slices).
func (h HazardEvent) Clone() HazardEvent {
	b, err := json.Marshal(h)
	if err != nil {
		return h
	}
	var c HazardEvent
	if err := json.Unmarshal(b, &c); err != nil {
		return h
	}
	return c
}
