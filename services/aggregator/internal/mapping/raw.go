// Package mapping turns raw BMKG/PVMBG JSON records into canonical HazardEvents.
//
// Schema policy for Problem 1 (tolerant reader, additive-only):
//
//  1. A record is decoded into a generic map, not a fixed struct, so an unknown
//     field can't be dropped silently or make the whole message fail to parse.
//  2. Fields with a canonical counterpart are read explicitly and validated
//     (required and typed). A missing or mistyped field, or an unknown enum
//     value, is not guessed. The record goes to quarantine (logged, counted,
//     listed in /internal/quarantine) and the rest of the batch continues.
//  3. Every other top-level field, including ones that appear later such as
//     confidence_level, is copied as-is into HazardEvent.attributes.
//
// Supported: new fields of any JSON type, and attribute-only fields disappearing.
// Not supported (quarantined): renaming or removing a field that has a canonical
// counterpart, changing its type, new enum values, changed meaning or units.
package mapping

import (
	"fmt"
	"strings"
	"time"
)

// MappingError marks a record that cannot be mapped (-> quarantine). It is
// distinct from a source failure (network, 5xx), which is the breaker's concern.
type MappingError struct {
	Field  string
	Reason string
}

func (e *MappingError) Error() string { return e.Field + ": " + e.Reason }

func bad(field, format string, args ...any) *MappingError {
	return &MappingError{Field: field, Reason: fmt.Sprintf(format, args...)}
}

func reqString(m map[string]any, k string) (string, error) {
	v, ok := m[k]
	if !ok || v == nil {
		return "", bad(k, "required field is missing")
	}
	s, ok := v.(string)
	if !ok {
		return "", bad(k, "expected string, got %T", v)
	}
	if strings.TrimSpace(s) == "" {
		return "", bad(k, "must not be empty")
	}
	return s, nil
}

func reqFloat(m map[string]any, k string) (float64, error) {
	v, ok := m[k]
	if !ok || v == nil {
		return 0, bad(k, "required field is missing")
	}
	f, ok := v.(float64)
	if !ok {
		return 0, bad(k, "expected number, got %T", v)
	}
	return f, nil
}

func reqTime(m map[string]any, k string) (time.Time, error) {
	s, err := reqString(m, k)
	if err != nil {
		return time.Time{}, err
	}
	t, perr := time.Parse(time.RFC3339Nano, s)
	if perr != nil {
		return time.Time{}, bad(k, "not an ISO 8601 datetime: %q", s)
	}
	// microsecond precision survives a PostgreSQL round-trip unchanged
	return t.UTC().Truncate(time.Microsecond), nil
}

// optTime parses an optional datetime; ok=false when absent or unparsable.
func optTime(m map[string]any, k string) (time.Time, bool) {
	s, isStr := m[k].(string)
	if !isStr {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC().Truncate(time.Microsecond), true
}

// splitExtras implements rule 3 of the policy: everything that is not in known
// is copied to the attributes map unchanged.
func splitExtras(raw map[string]any, known map[string]struct{}) map[string]any {
	extras := make(map[string]any)
	for k, v := range raw {
		if _, ok := known[k]; !ok {
			extras[k] = v
		}
	}
	return extras
}

func set(keys ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		m[k] = struct{}{}
	}
	return m
}

// RecordID returns a best-effort identifier of a raw record for log/quarantine.
func RecordID(raw map[string]any, keys ...string) string {
	for _, k := range keys {
		if s, ok := raw[k].(string); ok && s != "" {
			return s
		}
	}
	return ""
}
